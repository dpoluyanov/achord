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

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Camelion
 * @since 14/02/2018
 */
final class SendDataQueryContext<T> implements QueryContext {
    private static final int STATE_DISCONNECTED = 0;
    private static final int STATE_CONNECTED = 1;
    private static final int STATE_SERVER_INFO_RECEIVED = 2;
    private static final int STATE_SAMPLE_BLOCK_RECEIVED = 3;
    private final AuthData authData;
    private final String query;
    private final String queryId;
    private final Settings settings;
    private final Limits limits;
    private final Flow.Publisher<T[]> source;
    private final Channel channel;
    private final Flow.Subscriber<? super Void> s;
    private final EventLoopGroup workersGroup;
    private final AtomicInteger STATE = new AtomicInteger(STATE_DISCONNECTED);
    private ClickHouseServerInfo serverInfo;
    private GenericFutureListener<Future<? super Void>> catchErrorListener = future -> {
        if (!future.isSuccess()) {
            onChannelExceptionCaught(future.cause());
        }
    };

    SendDataQueryContext(AuthData authData, String queryId,
                         String query, Settings settings, Limits limits,
                         Channel channel,
                         Flow.Publisher<T[]> source, Flow.Subscriber<? super Void> s, EventLoopGroup workersGroup) {
        this.authData = authData;
        this.query = query;
        this.queryId = queryId;
        this.settings = settings;
        this.limits = limits;
        this.source = source;
        this.channel = channel;
        this.s = s;
        this.workersGroup = workersGroup;
    }

    @Override
    public AuthData getAuthData() {
        return authData;
    }

    @Override
    public void onChannelConnected() {
        int state;
        if ((state = STATE.compareAndExchange(STATE_DISCONNECTED, STATE_CONNECTED)) == STATE_DISCONNECTED) {
            channel.writeAndFlush(new HelloMessage(authData))
                    .addListener(catchErrorListener);
        } else {
            throw new IllegalStateException("Context expected to be in STATE_DISCONNEC8TED but got " + state);
        }
    }

    @Override
    public void onClickHouseServerInfoReceived(ClickHouseServerInfo serverInfo) {
        int state;
        if ((state = STATE.compareAndExchange(STATE_CONNECTED, STATE_SERVER_INFO_RECEIVED)) == STATE_CONNECTED) {
            this.serverInfo = serverInfo;

            channel.write(new SendQueryMessage(queryId, query, settings, limits, serverInfo.serverRevision))
                    .addListener(catchErrorListener);
            channel.writeAndFlush(DataBlock.EMPTY.retain())
                    .addListener(catchErrorListener);
        } else {
            throw new IllegalStateException("Context expected to be in STATE_CONNECTED but got " + state);
        }
    }

    @Override
    public void onDataBlockReceived(DataBlock block) {
        int state;
        if ((state = STATE.compareAndExchange(STATE_SERVER_INFO_RECEIVED, STATE_SAMPLE_BLOCK_RECEIVED)) == STATE_SERVER_INFO_RECEIVED) {
            // eventLoop for executing all onNext/onSubscribe operations
            EventLoop eventLoop = workersGroup.next();
            ObjectsToBlockProcessor<T> processor = new ObjectsToBlockProcessor<>(block, eventLoop, channel.alloc());
            DataBlockSender blockSender = new DataBlockSender(eventLoop);

            if (channel.pipeline().get(EmptyResponsePublisher.BLOCK_COMPRESSOR) != null) {
                channel.pipeline().addBefore(EmptyResponsePublisher.BLOCK_COMPRESSOR, "reactiveBlockSender", blockSender);
            } else {
                channel.pipeline().addFirst(blockSender);
            }
            eventLoop.execute(() -> {
                source.subscribe(processor);
                processor.subscribe(blockSender);
            });
        } else {
            throw new IllegalStateException("Context expected to be in STATE_SERVER_INFO_RECEIVED but got " + state);
        }
    }

    @Override
    public void onServerExceptionCaught(ClickHouseServerException exception) {
        s.onError(exception);
    }

    @Override
    public void onEndOfStream() {
        throw new IllegalStateException("End of stream should not be passed to SendDataQueryContext");
    }

    @Override
    public void onChannelExceptionCaught(Throwable cause) {
        s.onError(cause);
    }

    void completed() {
        s.onComplete();
    }
}
