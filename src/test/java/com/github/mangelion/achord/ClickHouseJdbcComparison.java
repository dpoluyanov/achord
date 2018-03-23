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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static com.github.mangelion.achord.ClickHouseClientTest.NUMBERS_COUNT;

/**
 * @author Camelion
 * @since 23.03.2018
 */
@Tag("jdbc-comparison")
@DockerContainer(image = "yandex/clickhouse-server", ports = {"8123:8123"})
final class ClickHouseJdbcComparison {
    private DataSource dataSource;

    @BeforeEach
    void beforeAll() {
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123/default");
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", waitStop = true, arguments = {
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
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", waitStop = true, arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test_uint64_3(date Date DEFAULT toDate(datetime), id UInt64, datetime UInt32, value UInt64) ENGINE = MergeTree(date, (datetime), 8192)"})
    void sendThreeColumns_jdbc() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (ClickHouseStatement stmt = (ClickHouseStatement) connection.createStatement()) {
                stmt.sendRowBinaryStream("INSERT INTO default.connection_test_uint64_3(id, datetime, value)",
                        stream -> {
                            for (int i = 0; i < NUMBERS_COUNT; i++) {
                                stream.writeUInt64(1);
                                stream.writeUInt32(1);
                                stream.writeUInt64(1);
                            }
                        });
            }
        }
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", waitStop = true, arguments = {
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
