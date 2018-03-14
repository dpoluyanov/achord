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

package com.github.mangelion;

import com.github.mangelion.test.extensions.docker.DockerContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.SynchronousSink;
import reactor.test.StepVerifier;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;
import static reactor.core.publisher.Flux.generate;

/**
 * @author Camelion
 * @since 11/02/2018
 */
@DockerContainer(image = "yandex/clickhouse-server", ports = {"9000:9000", "8123:8123"})
final class ClickHouseClientTest {
    private static final int NUMBERS_COUNT = 500 * 1024 * 1024;
    private ClickHouseClient client;
    private DataSource dataSource;

    @BeforeEach
    void beforeAll() {
        client = ClickHouseClient.bootstrap();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123/default");
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test_uint32(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes() {
        Object[] data = new Object[]{1};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.connection_test_uint32(value)",
                publisherToFlowPublisher(
                        generate((Consumer<SynchronousSink<Object[]>>) sink -> sink.next(data))
                                .take(NUMBERS_COUNT)));

        StepVerifier
                .create(flowPublisherToFlux(result))
                .verifyComplete();
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test_uint32(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes_withCompression() {
        client = client.compression(CompressionMethod.LZ4);

        Object[] data = new Object[]{1};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.connection_test_uint32(value)",
                publisherToFlowPublisher(
                        generate((Consumer<SynchronousSink<Object[]>>) sink -> sink.next(data))
                                .take(NUMBERS_COUNT)));

        StepVerifier
                .create(flowPublisherToFlux(result))
                .verifyComplete();
    }

    @Test
    @Tag("jdbc-comparison")
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test_uint32(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes_jdbc() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (ClickHouseStatement stmt = (ClickHouseStatement) connection.createStatement()) {
                stmt.sendRowBinaryStream("INSERT INTO default.connection_test_uint32(value)",
                        stream -> {
                            for (int i = 0; i < NUMBERS_COUNT; i++)
                                stream.writeUInt32(1);
                        });
            }
        }
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test_uint64(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt64) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallLongMultipleTimes_withCompression() {
        client = client.compression(CompressionMethod.LZ4);

        Object[] data = new Object[]{1L};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.connection_test_uint64(value)",
                publisherToFlowPublisher(
                        generate((Consumer<SynchronousSink<Object[]>>) sink -> sink.next(data))
                                .take(NUMBERS_COUNT)));

        StepVerifier
                .create(flowPublisherToFlux(result))
                .verifyComplete();
    }

    @Test
    @Tag("jdbc-comparison")
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test_uint64(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt64) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallLongMultipleTimes_jdbc() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (ClickHouseStatement stmt = (ClickHouseStatement) connection.createStatement()) {
                stmt.sendRowBinaryStream("INSERT INTO default.connection_test_uint64(value)",
                        stream -> {
                            for (int i = 0; i < NUMBERS_COUNT; i++)
                                stream.writeUInt64(1);
                        });
            }
        }
    }
}