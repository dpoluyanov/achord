package io.achord;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.util.concurrent.Future;

import java.util.concurrent.Flow;

import static io.achord.ClickHousePacketEncoder.CLICK_HOUSE_PACKET_ENCODER;
import static io.achord.ClickHouseServerMessageHandler.CLICK_HOUSE_SERVER_MESSAGE_HANDLER;
import static io.achord.DataBlockEncoder.DATA_BLOCK_ENCODER;
import static io.netty.channel.ChannelOption.TCP_NODELAY;

/**
 * @author Camelion
 * @since 10/02/2018
 */
public final class ClickHouseClient implements AutoCloseable {
    static final int COMPATIBLE_CLIENT_REVISION = 54327;
    static final String PACKET_DECODER = "decoder";
    static final String BLOCK_ENCODER = "blockEncoder";
    static final String PACKET_ENCODER = "encoder";

    private final Bootstrap b;
    private final DefaultEventLoop messageHandlerGroup;
    private final DefaultEventLoopGroup workersGroup;
    private String database;
    private String username = database = "default";
    private String password = "";
    private Settings settings = new Settings();
    private Limits limits = new Limits();

    public ClickHouseClient() {
        messageHandlerGroup = new DefaultEventLoop();
        // todo make № of threads customizable, like whole group
        workersGroup = new DefaultEventLoopGroup(2);
        b = new Bootstrap()
                // todo create I/O group selection (may be through property but prefer native)
                //.group(eventLoopGroup)
                // todo make configurable, because on macosx it shouldn't clash with Nagle Algorithm
                .option(TCP_NODELAY, true)
                .handler(new ChannelInitializer() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline()
                                // decoders
                                .addFirst(PACKET_DECODER, new ClickHousePacketDecoder())
                                .addLast(workersGroup, "messageHandler", CLICK_HOUSE_SERVER_MESSAGE_HANDLER)
                                // encoders
                                .addFirst(PACKET_ENCODER, CLICK_HOUSE_PACKET_ENCODER)
                                .addFirst(BLOCK_ENCODER, DATA_BLOCK_ENCODER);
                    }
                });
    }

    /**
     * Creates new client for ClickHouse server
     *
     * @return created client
     */
    static ClickHouseClient bootstrap() {
        return new ClickHouseClient();
    }

    public ClickHouseClient address(String inetHost, int port) {
        b.remoteAddress(inetHost, port);
        return this;
    }

    public ClickHouseClient database(String database) {
        this.database = database;
        return this;
    }

    public ClickHouseClient username(String username) {
        this.username = username;
        return this;
    }

    public ClickHouseClient password(String password) {
        this.password = password;
        return this;
    }

    public Flow.Publisher<Void> sendData(String query, CHObjectsStorage storage) {
        return null;
    }

    public Flow.Publisher<Void> sendData(String query, Flow.Publisher<Object[]> source) {
        return this.sendData("", query, source);
    }

    /**
     * Reactive way for sending data.
     * Data object {@code Object[]} collects from upstream and buffers for sending latter in huge blocks
     *
     * @param query  CH query description of inserted data
     * @param source reactive data publisher
     * @return empty {@code <Vo  id>} publisher that signals success or error after insert process ends
     */
    public Flow.Publisher<Void> sendData(String query, String queryId, Flow.Publisher<Object[]> source) {
        AuthData authData = new AuthData(database, username, password);
        return new EmptyResponsePublisher(b.clone(), workersGroup, authData, query, queryId, settings, limits, source);
    }

    @Override
    public void close() {
        Future<?> configShutdown = b.config().group().shutdownGracefully();
        Future<?> messageHandlerShutdown = messageHandlerGroup.shutdownGracefully();
        Future<?> decompressingShutdown = workersGroup.shutdownGracefully();

        configShutdown.syncUninterruptibly();
        decompressingShutdown.syncUninterruptibly();
        messageHandlerShutdown.syncUninterruptibly();
    }
}
