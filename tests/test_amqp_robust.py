import asyncio
import logging
from socket import socket
from typing import Optional

import aiormq
from contextlib import suppress

from aiormq import ChannelLockedResource

from aio_pika import connect_robust, Message
from aio_pika.robust_channel import RobustChannel
from aio_pika.robust_connection import RobustConnection
from aio_pika.robust_queue import RobustQueue
from tests import AMQP_URL
from tests.test_amqp import TestCase as AMQPTestCase


class Proxy:
    CHUNK_SIZE = 1500

    def __init__(self, *, loop, shost='127.0.0.1', sport,
                 dhost='127.0.0.1', dport):

        self.loop = loop

        self.server = None
        self.src_host = shost
        self.src_port = sport
        self.dst_host = dhost
        self.dst_port = dport
        self.connections = set()

    async def _pipe(self, reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                writer.write(await reader.read(self.CHUNK_SIZE))
        finally:
            writer.close()

    async def handle_client(self, creader: asyncio.StreamReader,
                            cwriter: asyncio.StreamWriter):
        sreader, swriter = await asyncio.open_connection(
            host=self.dst_host,
            port=self.dst_port,
            loop=self.loop,
        )

        self.connections.add(swriter)
        self.connections.add(cwriter)

        await asyncio.wait([
            self._pipe(sreader, cwriter),
            self._pipe(creader, swriter),
        ])

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_client,
            host=self.src_host,
            port=self.src_port,
            loop=self.loop,
        )

        return self.server

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()

    async def disconnect(self):
        tasks = list()

        async def close(w):
            w.close()
            await w.wait_closed()

        while self.connections:
            writer: asyncio.StreamWriter = self.connections.pop()
            tasks.append(self.loop.create_task(close(writer)))

        await asyncio.wait(tasks)


class TestCase(AMQPTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy: Optional[Proxy] = None

    @staticmethod
    def get_unused_port() -> int:
        sock = socket()
        sock.bind(('', 0))
        port = sock.getsockname()[-1]
        sock.close()
        return port

    async def create_connection(self, cleanup=True):
        self.proxy = Proxy(
            dhost=AMQP_URL.host,
            dport=AMQP_URL.port,
            sport=self.get_unused_port(),
            loop=self.loop,
        )

        await self.proxy.start()

        url = AMQP_URL.with_host(
            self.proxy.src_host
        ).with_port(
            self.proxy.src_port
        ).update_query(reconnect_interval=1)

        client = await connect_robust(str(url), loop=self.loop, reconnect_interval=0.1)

        if cleanup:
            self.addCleanup(client.close)
            self.addCleanup(self.proxy.disconnect)

        return client

    async def test_set_qos(self):
        channel = await self.create_channel()
        await channel.set_qos(prefetch_count=1)

    async def test_revive_passive_queue_on_reconnect(self):
        client1 = await self.create_connection()
        self.assertIsInstance(client1, RobustConnection)

        client2 = await self.create_connection()
        self.assertIsInstance(client2, RobustConnection)

        reconnect_event = asyncio.Event()
        reconnect_count = 0

        def reconnect_callback(conn):
            nonlocal reconnect_count
            reconnect_count += 1
            reconnect_event.set()
            reconnect_event.clear()

        client2.add_reconnect_callback(reconnect_callback)

        queue_name = self.get_random_name()
        channel1 = await client1.channel()
        self.assertIsInstance(channel1, RobustChannel)

        channel2 = await client2.channel()
        self.assertIsInstance(channel2, RobustChannel)

        queue1 = await self.declare_queue(
            queue_name,
            auto_delete=False,
            passive=False,
            channel=channel1
        )
        self.assertIsInstance(queue1, RobustQueue)

        queue2 = await self.declare_queue(
            queue_name,
            passive=True,
            channel=channel2
        )
        self.assertIsInstance(queue2, RobustQueue)

        await client2.connection.close(aiormq.AMQPError(320, 'Closed'))

        await reconnect_event.wait()

        self.assertEqual(reconnect_count, 1)

        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                reconnect_event.wait(),
                client2.reconnect_interval * 2
            )

        self.assertEqual(reconnect_count, 1)

    async def test_robust_reconnect__no_downtime(self):
        await RobustConnectTester(self, proxy_downtime=0).run()

    async def test_robust_reconnect__with_downtime(self):
        await RobustConnectTester(self, proxy_downtime=2).run()

    async def test_channel_locked_resource2(self):
        ch1 = await self.create_channel()
        ch2 = await self.create_channel()

        qname = self.get_random_name("channel", "locked", "resource")

        q1 = await ch1.declare_queue(qname, exclusive=True, robust=False)
        await q1.consume(print, exclusive=True)

        with self.assertRaises(ChannelLockedResource):
            q2 = await ch2.declare_queue(qname, exclusive=True, robust=False)
            await q2.consume(print, exclusive=True)


class RobustConnectTester:
    def __init__(self, test_case: TestCase, *, proxy_downtime: int):
        self._test_case = test_case
        self._proxy_downtime = proxy_downtime

    async def _reader(self, queue, consumed):
        async with queue.iterator() as q:
            async for message in q:
                consumed.append(message)
                await message.ack()

    async def _writer(self, channel, queue):
        for _ in range(1000):
            await channel.default_exchange.publish(
                Message(b''), queue.name,
            )

        return

        while True:
            try:
                await channel.default_exchange.publish(
                    Message(b''), queue.name,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(0.1)

    async def _restart_proxy(self):
        proxy: Proxy = self._test_case.proxy

        await proxy.disconnect()
        if self._proxy_downtime:
            await proxy.stop()
            await asyncio.sleep(self._proxy_downtime)
            await proxy.start()

    async def run(self):
        channel1 = await self._test_case.create_channel()
        channel2 = await self._test_case.create_channel()

        consumed = []
        queue = await channel1.declare_queue()

        reader_task = asyncio.get_event_loop().create_task(
            self._reader(queue, consumed)
        )
        self._test_case.addCleanup(reader_task.cancel)

        writer_task = asyncio.get_event_loop().create_task(
            self._writer(channel2, queue)
        )
        self._test_case.addCleanup(writer_task.cancel)
        await writer_task

        logging.info("Wait for some messages to be processed")
        while len(consumed) < 10:
            await asyncio.sleep(0.01)

        logging.info("Disconnect all clients")
        await self._restart_proxy()

        logging.info("Waiting for reconnect")
        await asyncio.sleep(5)

        logging.info("Waiting connections")
        await asyncio.wait([
            channel1._connection.ready(),
            channel2._connection.ready()
        ])

        while len(consumed) < 30:
            await asyncio.sleep(0.1)

        assert len(consumed) >= 30
