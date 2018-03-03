package io.achord;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.achord.BlockCompressingHandler.BLOCK_COMPRESSING_HANDLER;
import static io.achord.BlockDecompressingHandler.BLOCK_DECOMPRESSING_HANDLER;
import static io.achord.ClickHouseClient.BLOCK_ENCODER;
import static io.achord.ClickHouseClient.PACKET_DECODER;
import static io.achord.ClickHousePacketDecoder.CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE;
import static io.achord.ClickHousePacketDecoder.CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE;
import static io.achord.QueryContext.QUERY_CONTEXT_ATTR;

/**
 * @author Camelion
 * @since 14/02/2018
 */
final class EmptyResponsePublisher implements Flow.Publisher<Void> {
    static final String BLOCK_COMPRESSOR = "blockCompressor";
    private final Bootstrap bootstrap;
    private final AuthData authData;
    private final String query;
    private final String queryId;
    private final Settings settings;
    private final Limits limits;
    private final Flow.Publisher<Object[]> source;
    private final DefaultEventLoopGroup workersGroup;

    EmptyResponsePublisher(Bootstrap bootstrap, DefaultEventLoopGroup workersGroup,
                           AuthData authData, String query, String queryId, Settings settings, Limits limits,
                           Flow.Publisher<Object[]> source) {
        this.bootstrap = bootstrap;
        this.workersGroup = workersGroup;
        this.authData = authData;
        this.query = query;
        this.queryId = queryId;
        this.settings = settings;
        this.limits = limits;
        this.source = source;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Void> subscriber) {
        subscriber.onSubscribe(new Subscription(subscriber));
    }

    final class Subscription implements Flow.Subscription, GenericFutureListener<Future<? super Void>> {
        private final AtomicBoolean WIP = new AtomicBoolean();
        private final Flow.Subscriber<? super Void> s;
        volatile ChannelFuture cf;


        private Subscription(Flow.Subscriber<? super Void> s) {
            this.s = s;
        }

        // tries to connect to channel after first non-zero request has came
        @Override
        public synchronized void request(long n) {
            if (!WIP.get() && n > 0 && WIP.compareAndSet(false, true)) {
                cf = bootstrap.connect();
                cf.channel().attr(QUERY_CONTEXT_ATTR)
                        .set(new SendDataQueryContext(authData, query, queryId, settings, limits, cf.channel(), source, s,
                                workersGroup));

                if (settings.isCompressionEnabled()) {
                    cf.channel().attr(CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE).set(settings.getNetworkCompressionMethod());
                    cf.channel().attr(CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE).set(settings.getNetworkZstdCompressionLevel());
                    cf.channel().pipeline().remove(BLOCK_ENCODER);

                    cf.channel().pipeline()
                            .addFirst(workersGroup, BLOCK_COMPRESSOR, BLOCK_COMPRESSING_HANDLER)
                            .addAfter(workersGroup, PACKET_DECODER, "blockDecompressor", BLOCK_DECOMPRESSING_HANDLER);
                }

                cf.addListener(this);
            }
        }

        @Override
        public synchronized void cancel() {
            if (cf != null) {
                cf.removeListener(this)
                        .addListener(future -> cf.channel().close().syncUninterruptibly())
                        .syncUninterruptibly();
            }
        }

        // invokes when channel became connected or connection fails
        @Override
        public void operationComplete(Future<? super Void> future) {
            if (future.isSuccess()) {
                QueryContext context = cf.channel().attr(QUERY_CONTEXT_ATTR).get();
                context.onChannelConnected();
            } else {
                s.onError(future.cause());
            }
        }
    }
}
