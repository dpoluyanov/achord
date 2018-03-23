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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.mangelion.achord.BlockCompressingHandler.BLOCK_COMPRESSING_HANDLER;
import static com.github.mangelion.achord.BlockDecompressingHandler.BLOCK_DECOMPRESSING_HANDLER;
import static com.github.mangelion.achord.ClickHousePacketDecoder.CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE;
import static com.github.mangelion.achord.ClickHousePacketDecoder.CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE;
import static com.github.mangelion.achord.QueryContext.QUERY_CONTEXT_ATTR;

/**
 * @author Camelion
 * @since 14/02/2018
 */
final class EmptyResponsePublisher<T> implements Flow.Publisher<Void> {
    static final String BLOCK_COMPRESSOR = "blockCompressor";
    private final Bootstrap bootstrap;
    private final AuthData authData;
    private final String query;
    private final String queryId;
    private final Settings settings;
    private final Limits limits;
    private final Flow.Publisher<T[]> source;
    private final EventLoopGroup workersGroup;
    private final EventLoopGroup compressionGroup;

    EmptyResponsePublisher(Bootstrap bootstrap, EventLoopGroup workersGroup, EventLoopGroup compressionGroup,
                           AuthData authData, String queryId, String query, Settings settings, Limits limits,
                           Flow.Publisher<T[]> source) {
        this.bootstrap = bootstrap;
        this.workersGroup = workersGroup;
        this.compressionGroup = compressionGroup;
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
                        .set(new SendDataQueryContext<>(authData, queryId, query, settings, limits, cf.channel(), source, s,
                                workersGroup));

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
                // set additional settings
                if (settings.isCompressionEnabled()) {
                    cf.channel().attr(CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE).set(settings.getNetworkCompressionMethod());
                    cf.channel().attr(CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE).set(settings.getNetworkZstdCompressionLevel());
                    cf.channel().pipeline().remove(ClickHouseClient.BLOCK_ENCODER);

                    cf.channel().pipeline()
                            .addFirst(compressionGroup, BLOCK_COMPRESSOR, BLOCK_COMPRESSING_HANDLER)
                            .addAfter(compressionGroup, ClickHouseClient.PACKET_DECODER, "blockDecompressor", BLOCK_DECOMPRESSING_HANDLER);
                }

                QueryContext context = cf.channel().attr(QUERY_CONTEXT_ATTR).get();
                context.onChannelConnected();
            } else {
                s.onError(future.cause());
            }
        }
    }
}
