package io.achord;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import static io.achord.EmptyResponsePublisher.BLOCK_COMPRESSOR;

/**
 * @author Camelion
 * @since 14/02/2018
 */
final class SendDataQueryContext implements QueryContext {
    private static final int STATE_DISCONNECTED = 0;
    private static final int STATE_CONNECTED = 1;
    private static final int STATE_SERVER_INFO_RECEIVED = 2;
    private static final int STATE_SAMPLE_BLOCK_RECEIVED = 3;
    private final AuthData authData;
    private final String query;
    private final String queryId;
    private final Settings settings;
    private final Limits limits;
    private final Flow.Publisher<Object[]> source;
    private final Channel channel;
    private final Flow.Subscriber<? super Void> s;
    private final EventLoopGroup workersGroup;
    private final AtomicInteger STATE = new AtomicInteger(STATE_DISCONNECTED);
    private ClickHouseServerInfo serverInfo;

    SendDataQueryContext(AuthData authData, String query,
                         String queryId, Settings settings, Limits limits,
                         Channel channel,
                         Flow.Publisher<Object[]> source, Flow.Subscriber<? super Void> s, EventLoopGroup workersGroup) {
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
            channel.writeAndFlush(new HelloMessage(authData));
        } else {
            throw new IllegalStateException("Context expected to be in STATE_DISCONNEC8TED but got " + state);
        }
    }

    @Override
    public void onClickHouseServerInfoReceived(ClickHouseServerInfo serverInfo) {
        int state;
        if ((state = STATE.compareAndExchange(STATE_CONNECTED, STATE_SERVER_INFO_RECEIVED)) == STATE_CONNECTED) {
            this.serverInfo = serverInfo;

            channel.write(new SendQueryMessage(queryId, query, settings, limits, serverInfo.serverRevision));
            channel.writeAndFlush(DataBlock.EMPTY);
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
            ObjectsToBlockProcessor processor = new ObjectsToBlockProcessor(block, channel.alloc());
            DataBlockSender blockSender = new DataBlockSender(eventLoop);

            if (channel.pipeline().get(BLOCK_COMPRESSOR) != null) {
                channel.pipeline().addBefore(BLOCK_COMPRESSOR, "reactiveBlockSender", blockSender);
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
        s.onComplete();
    }

    @Override
    public void onChannelExceptionCaught(Throwable cause) {
        s.onError(cause);
    }
}
