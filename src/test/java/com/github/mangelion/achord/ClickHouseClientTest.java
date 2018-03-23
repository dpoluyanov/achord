/*
 * Copyright 2017-2018 Mangelion
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mangelion.achord;

import com.github.mangelion.test.extensions.docker.DockerContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.Flow;
import java.util.function.Consumer;

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;
import static reactor.core.publisher.Flux.generate;

/**
 * @author Camelion
 * @since 11/02/2018
 * Checks integration with ClickHouse server. Could be considered as slow IT tests.
 */
@DockerContainer(image = "yandex/clickhouse-server", ports = {"9000:9000"})
final class ClickHouseClientTest {
    static final int NUMBERS_COUNT = 64 * 1024 * 1024;
    private ClickHouseClient client;

    @BeforeEach
    void beforeAll() {
        client = ClickHouseClient.bootstrap();
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", waitStop = true, arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.sendSmallIntMultipleTimes(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes() {
        Object[] data = new Object[]{1};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.sendSmallIntMultipleTimes(value)",
                publisherToFlowPublisher(
                        generate((Consumer<SynchronousSink<Object[]>>) sink -> sink.next(data))
                                .take(NUMBERS_COUNT)));

        flowPublisherToFlux(result).blockLast();
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", waitStop = true, arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.sendSmallIntMultipleTimes_withCompression(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes_withCompression() {
        client = client.compression(CompressionMethod.LZ4);

        Object[] data = new Object[]{1};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.sendSmallIntMultipleTimes_withCompression(value)",
                publisherToFlowPublisher(
                        generate((Consumer<SynchronousSink<Object[]>>) sink -> sink.next(data))
                                .take(NUMBERS_COUNT)));

        flowPublisherToFlux(result).blockLast();
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", waitStop = true, arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.sendSmallLongMultipleTimes_withCompression(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt64) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallLongMultipleTimes_withCompression() {
        client = client.compression(CompressionMethod.LZ4);

        Object[] data = new Object[]{1L};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.sendSmallLongMultipleTimes_withCompression(value)",
                publisherToFlowPublisher(
                        generate((Consumer<SynchronousSink<Object[]>>) sink -> sink.next(data))
                                .take(NUMBERS_COUNT)));

        flowPublisherToFlux(result).blockLast();
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", waitStop = true, arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.sendThreeColumnsMultipleTimes_withCompression(date Date DEFAULT toDate(datetime), id UInt64, datetime UInt32, value UInt64) ENGINE = MergeTree(date, (datetime), 8192)"})
    void sendThreeColumnsMultipleTimes_withCompression() {
        client = client.compression(CompressionMethod.LZ4);

        Object[] data = new Object[]{1L, 1, 1L};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.sendThreeColumnsMultipleTimes_withCompression(id, datetime, value)",
                publisherToFlowPublisher(
                        generate((Consumer<SynchronousSink<Object[]>>) sink -> sink.next(data))
                                .take(NUMBERS_COUNT)));

        flowPublisherToFlux(result).blockLast();
    }
}