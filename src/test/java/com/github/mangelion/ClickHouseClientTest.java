package com.github.mangelion;

import com.github.mangelion.test.extensions.docker.DockerContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Flow;

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;
import static reactor.core.publisher.Flux.range;

/**
 * @author Camelion
 * @since 11/02/2018
 */
@DockerContainer(image = "yandex/clickhouse-server", ports = {"9000:9000", "8123:8123"})
final class ClickHouseClientTest {
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
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes() {
        Object[] data = new Object[]{1};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.connection_test(value)",
                publisherToFlowPublisher(
                        range(0, 500 * 1024 * 1024)
                                .map(i -> data)));

        StepVerifier
                .create(flowPublisherToFlux(result))
                .verifyComplete();
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes_withCompression() {
        client = client.compression(CompressionMethod.LZ4);

        Object[] data = new Object[]{1};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.connection_test(value)",
                publisherToFlowPublisher(
                        range(0, 500 * 1024 * 1024)
                                .map(i -> data)));

        StepVerifier
                .create(flowPublisherToFlux(result))
                .verifyComplete();
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test(date Date DEFAULT toDate(datetime), datetime DateTime DEFAULT now(), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes_jdbc() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (ClickHouseStatement stmt = (ClickHouseStatement) connection.createStatement()) {
                stmt.sendRowBinaryStream("INSERT INTO default.connection_test(value)",
                        stream -> {
                            for (int i = 0; i < 500 * 1024 * 1024; i++)
                                stream.writeUInt32(1);
                        });
            }
        }
    }
}