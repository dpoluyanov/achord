package io.achord;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import org.jctools.queues.SpscArrayQueue;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author Camelion
 * @since 01/03/2018
 */
final class DataBlockSender extends ChannelInboundHandlerAdapter implements Flow.Subscriber<DataBlock> {
    private static final int ERROR = -2;
    private static final int UNSUBSCRIBED = -1;
    private static final int SUBSCRIBED = 0;
    private static final int COMPLETE = 1;
    // TODO: prefetch should be configurable and (may be) automatically calculable by some evristics
    private static final int PREFETCH = 2 << 3;
    private static final AtomicIntegerFieldUpdater<DataBlockSender> STATE =
            AtomicIntegerFieldUpdater.newUpdater(DataBlockSender.class, "state");
    private final AtomicInteger REQUESTED = new AtomicInteger();
    private final SpscArrayQueue<DataBlock> queue = new SpscArrayQueue<>(PREFETCH);
    private final EventLoopGroup eventLoop;
    private volatile Flow.Subscription subscription;
    private volatile ChannelHandlerContext ctx;
    private volatile int state = UNSUBSCRIBED;

    DataBlockSender(EventLoopGroup eventLoopGroup) {
        this.eventLoop = eventLoopGroup;
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
            int r = REQUESTED.decrementAndGet();
            assert r >= 0;

            ctx.write(b);
            written++;
        }

        if (written > 0) {
            ctx.flush();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (subscription != null) {
            subscription.cancel();
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            drain();
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        if (STATE.compareAndSet(this, UNSUBSCRIBED, SUBSCRIBED)) {
            REQUESTED.set(PREFETCH);
            subscription.request(PREFETCH);
        } else {
            throw new IllegalStateException("Unexpected state on onSubscribe()");
        }
    }

    @Override
    public void onNext(DataBlock item) {
        if (state == SUBSCRIBED) {
            if (ctx != null && ctx.channel().isWritable()) {
                ctx.writeAndFlush(item);
                subscription.request(1);
            } else {
                // put to queue from single thread
                eventLoop.execute(() -> queue.offer(item));
            }
        } else {
            throw new IllegalStateException("Unexpected state on onNext()");
        }
    }

    @Override
    public void onError(Throwable throwable) {
        STATE.set(this, ERROR);
        ctx.channel().close().syncUninterruptibly();
    }

    @Override
    public void onComplete() {
        if (STATE.compareAndSet(this, SUBSCRIBED, COMPLETE)) {
            ctx.channel().eventLoop().execute(() -> {
                // push all blocks into channel and close it
                drain();
                ctx.channel().close();
            });
        } else {
            throw new IllegalStateException("Unexpected state on onComplete()");
        }
    }
}
