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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import org.jctools.queues.SpscArrayQueue;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.github.mangelion.achord.QueryContext.QUERY_CONTEXT_ATTR;


/**
 * @author Camelion
 * @since 01/03/2018
 */
final class DataBlockSender extends ChannelInboundHandlerAdapter implements Flow.Subscriber<DataBlock> {
    private static final int ERROR = -2;
    private static final int UNSUBSCRIBED = -1;
    private static final int SUBSCRIBED = 0;
    private static final int COMPLETE = 1;
    private static final int TERMINATED = 2;
    // TODO: prefetch should be configurable and (may be) automatically calculable by some evristics
    private static final int PREFETCH = 2 << 3;
    private static final AtomicIntegerFieldUpdater<DataBlockSender> STATE =
            AtomicIntegerFieldUpdater.newUpdater(DataBlockSender.class, "state");
    private final AtomicInteger REQUESTED = new AtomicInteger();
    private final SpscArrayQueue<DataBlock> queue = new SpscArrayQueue<>(PREFETCH);
    private final EventLoop eventLoop;
    private volatile Flow.Subscription subscription;
    private volatile ChannelHandlerContext ctx;
    private volatile int state = UNSUBSCRIBED;

    DataBlockSender(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        drain();
    }

    /**
     * Always executes by netty code
     */
    private void drain() {
        DataBlock b;
        int written = 0;
        while (ctx.channel().isWritable() && (b = queue.poll()) != null) {
            ctx.channel().write(b).addListener(future -> {
                if (future.isSuccess()) {
                    REQUESTED.decrementAndGet();
                    requestNext();
                } else {
                    onError(future.cause());
                }
            });
            written++;
        }

        if (written > 0) {
            ctx.channel().flush();
        }
    }

    private void requestNext() {
        if (state == SUBSCRIBED) {
            if (REQUESTED.getAndIncrement() <= PREFETCH)
                if (eventLoop.inEventLoop()) {
                    subscription.request(1);
                } else {
                    eventLoop.execute(() -> {
                        // recheck after execution
                        if (state == SUBSCRIBED) {
                            subscription.request(1);
                        }
                    });
                }
            else {
                throw new IllegalStateException("Requested count is overgrowth");
            }
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        STATE.set(this, TERMINATED);
        queue.drain(ReferenceCountUtil::release);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            drain();
        }
        super.channelWritabilityChanged(ctx);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        if (STATE.compareAndSet(this, UNSUBSCRIBED, SUBSCRIBED)) {
            REQUESTED.set(PREFETCH);
            if (eventLoop.inEventLoop()) {
                subscription.request(PREFETCH);
            } else {
                eventLoop.execute(() -> subscription.request(PREFETCH));
            }
        } else {
            throw new IllegalStateException("Unexpected state on onSubscribe()");
        }
    }

    @Override
    public void onNext(DataBlock item) {
        if (state == SUBSCRIBED) {
            if (ctx != null && ctx.channel().isWritable()) {
                ctx.channel().writeAndFlush(item).addListener(future -> {
                    if (future.isSuccess()) {
                        REQUESTED.decrementAndGet();
                        requestNext();
                    } else {
                        onError(future.cause());
                    }
                });
            } else {
                // put to queue from single thread
                if (eventLoop.inEventLoop()) {
                    if (!queue.offer(item)) {
                        eventuallyOffer(item);
                    }
                } else {
                    eventuallyOffer(item);
                }
            }
        } else {
            // consume
            ReferenceCountUtil.release(item);
            throw new IllegalStateException("Unexpected state on onNext()");
        }
    }

    @Override
    public void onError(Throwable throwable) {
        subscription.cancel();
        STATE.set(this, ERROR);
        ctx.fireExceptionCaught(throwable);
    }

    @Override
    public void onComplete() {
        if (STATE.compareAndSet(this, SUBSCRIBED, COMPLETE)) {
            ctx.channel().eventLoop().execute(() -> {
                // push all blocks into channel and close it
                drain();

                QueryContext queryContext = ctx.channel().attr(QUERY_CONTEXT_ATTR).get();

                ((SendDataQueryContext) queryContext).completed();
            });
        } else {
            throw new IllegalStateException("Unexpected state on onComplete()");
        }
    }

    private void eventuallyOffer(DataBlock item) {
        eventLoop.execute(() -> {
            if (!queue.offer(item)) {
                eventuallyOffer(item);
            }
        });
    }
}
