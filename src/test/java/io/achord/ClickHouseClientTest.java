package io.achord;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.concurrent.Flow;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;
import static reactor.core.publisher.Flux.range;

/**
 * @author Camelion
 * @since 11/02/2018
 */
final class ClickHouseClientTest {
    static ClickHouseClient client;

    @BeforeAll
    static void beforeAll() {
        client = ClickHouseClient.bootstrap();
    }

    @Test
    void sendSmallIntMultipleTimes() {
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.FINE);
        for (Handler h : rootLogger.getHandlers()) {
            h.setLevel(Level.FINE);
        }

        Object[] data = new Object[]{1};

        Flow.Publisher<Void> result = client.sendData("INSERT INTO test.connection_test(value)",
                publisherToFlowPublisher(
                        range(Integer.MIN_VALUE, Integer.MAX_VALUE)
                                .map(i -> data)
                ));

        StepVerifier
                .create(flowPublisherToFlux(result))
                .verifyComplete();
    }
}