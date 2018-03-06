package io.achord;

import io.achord.test.extensions.docker.DockerContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.concurrent.Flow;

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;
import static reactor.core.publisher.Flux.range;

/**
 * @author Camelion
 * @since 11/02/2018
 */
@DockerContainer(image = "yandex/clickhouse-server", ports = "9000:9000")
final class ClickHouseClientTest {
    static ClickHouseClient client;

    @BeforeAll
    static void beforeAll() {
        client = ClickHouseClient.bootstrap();
    }

    @Test
    @DockerContainer(image = "yandex/clickhouse-client", net = "host", arguments = {
            "--multiquery",
            "--query=CREATE TABLE IF NOT EXISTS default.connection_test(date Date DEFAULT toDate(now()), value UInt32) ENGINE = MergeTree(date, (date), 8192)"})
    void sendSmallIntMultipleTimes() {
        Object[] data = new Object[]{1};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO default.connection_test(value)",
                publisherToFlowPublisher(
                        range(0, 10 * 1024 * 1024)
                                .map(i -> data)
                ));

        StepVerifier
                .create(flowPublisherToFlux(result))
                .verifyComplete();
    }
}