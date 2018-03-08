package io.achord;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import static io.achord.DataBlock.EMPTY;

/**
 * @author Camelion
 * @since 19/02/2018
 * In counterpart to this we need a Storage-Based processor, that can be more effective
 */
final class ObjectsToBlockProcessor<T> implements Flow.Processor<T[], DataBlock> {
    private static final int BLOCK_SIZE = 1024 * 1024;
    private static final int UNSUBSCRIBED = -1;
    private static final int SUBSCRIBED = 0;
    private static final int WIP = 1;
    private static final int COMPLETED = 2;
    private static final int ERROR = 3;
    private static final int CANCELLED = 4;
    private static final AtomicIntegerFieldUpdater<ObjectsToBlockProcessor> STATE =
            AtomicIntegerFieldUpdater.newUpdater(ObjectsToBlockProcessor.class, "state");

    private final AtomicBoolean bufferReleased = new AtomicBoolean();
    private final ByteBufAllocator alloc;
    private final EventLoop eventLoop;
    private final AtomicLong requested = new AtomicLong();
    volatile DataBlock sample;
    private volatile Flow.Subscription subscription;
    private volatile int state = UNSUBSCRIBED;
    private volatile Flow.Subscriber<? super DataBlock> subscriber;
    private volatile ColumnWithTypeAndName[] columns;
    private volatile int rows;

    ObjectsToBlockProcessor(DataBlock sample, EventLoop eventLoop, ByteBufAllocator alloc) {
        this.sample = sample;
        this.eventLoop = eventLoop;
        this.alloc = alloc;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super DataBlock> subscriber) {
        this.subscriber = subscriber;
        recreateColumns();
        if (STATE.compareAndSet(this, SUBSCRIBED, WIP)) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (state == WIP) {
                        // suppose that in future some number of buffers should be on-the-fly for immediately sending after request
                        requested.addAndGet(n * BLOCK_SIZE);

                        // request data for n-next blocks
                        subscription.request(n * BLOCK_SIZE);
                    } else {
                        onError(new IllegalStateException("request(n) come to unexpected size"));
                    }
                }

                @Override
                public void cancel() {
                    int s;
                    for (; ; ) {
                        if ((s = state) == WIP || s == SUBSCRIBED) {
                            if (STATE.compareAndSet(ObjectsToBlockProcessor.this, s, CANCELLED)) {
                                subscription.cancel();
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            });
        } else {
            freeBuffers();
            throw new IllegalStateException("onSubscribe come to unexpected state");
        }
    }


    private void recreateColumns() {
        ColumnWithTypeAndName[] cs = new ColumnWithTypeAndName[sample.columns.length];
        for (int i = 0; i < sample.columns.length; i++) {
            // todo: data for buffers can be written independently (by some bunch of buffers)
            // and then collected into composite buffer (before forming resulting dataBlock)
            ByteBuf data = alloc.directBuffer();
            cs[i] = new ColumnWithTypeAndName(sample.columns[i].type, sample.columns[i].name, data);
        }
        columns = cs;
        rows = 0;
    }

    private void freeBuffers() {
        if (bufferReleased.compareAndSet(false, true)) {
            if (sample != null) {
                ReferenceCountUtil.release(sample);
                sample = null;
            }
            if (columns != null) {
                for (int i = 0; i < columns.length; i++) {
                    // columns[i] can be null if error happens on creation stage
                    if (columns[i] != null && columns[i].data.refCnt() > 0)
                        // release once
                        ReferenceCountUtil.release(columns[i].data);
                }

                columns = null;
            }
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        try {
            if (STATE.compareAndSet(this, UNSUBSCRIBED, SUBSCRIBED)) {
                this.subscription = subscription;
            } else {
                throw new IllegalStateException("State expected to be unsubscribed");
            }
        } catch (Throwable e) {
            freeBuffers();
            throw e;
        }
    }

    // we can do better without synchronization and with avoiding all contention problems but something later
    // with another algorithm that could be applied as below:
    //      1) write into current block under cas
    //      2) if cas write is failed create new block and write into (but keep the same counter)
    //      3) after rows counter exceeded threshold (1024 * 1024) switch this couple of blocks into merge state
    //         note: neither new writes can be done to this blocks.
    //      4) when last thread writes into blocks in merge state (probably should be controlled by another counter)
    //         the thread pushes this blocks into subscriber onNext chain
    @Override
    public synchronized void onNext(T[] item) {
        try {
            if (state == WIP) {
                long left;
                if ((left = requested.decrementAndGet()) >= 0) {
                    for (int i = 0; i < columns.length; i++) {
                        ColumnWithTypeAndName c = columns[i];
                        c.type.write(c.data, item[i]);
                    }

                    if (++rows >= BLOCK_SIZE) {
                        DataBlock block = new DataBlock(sample.info, columns, rows);

                        try {
                            subscriber.onNext(block);
                        } catch (Throwable e) {
                            // block can does not reach any channel handler
                            if (block.refCnt() > 0)
                                ReferenceCountUtil.release(block);
                            throw e;
                        }
                        recreateColumns();
                    }
                } else {
                    throw new IllegalStateException("onNext produces unexpected count of elements");
                }
            } else {
                // possible should be treated as ignore
                throw new IllegalStateException("onNext passed to illegal state");
            }
        } catch (Throwable e) {
            onError(e);
        }
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        try {
            if (STATE.compareAndSet(this, WIP, ERROR)) {
                subscription.cancel();
                subscriber.onError(throwable);
            }
        } finally {
            freeBuffers();
        }
    }

    @Override
    public synchronized void onComplete() {
        try {
            if (STATE.compareAndSet(this, WIP, COMPLETED)) {
                if (rows > 0) {
                    subscriber.onNext((DataBlock) EMPTY.retain());
                }
                subscriber.onComplete();
            }
        } finally {
            freeBuffers();
        }
    }
}
