package com.github.mangelion;

import com.github.mangelion.Settings.SettingCompressionMethod;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;

import java.util.concurrent.Flow;

import static com.github.mangelion.ClickHousePacketEncoder.CLICK_HOUSE_PACKET_ENCODER;
import static com.github.mangelion.Settings.NETWORK_COMPRESSION_METHOD;
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
    private final EventLoopGroup workersGroup;
    private final EventLoopGroup compressionGroup;
    private String database;
    private String username = database = "default";
    private String password = "";
    private Settings settings = new Settings();
    private Limits limits = new Limits();
    private CompressionMethod compressionMethod;

    public ClickHouseClient() {
        // todo make № of threads customizable, like whole group
        workersGroup = new DefaultEventLoopGroup(2);
        compressionGroup = new DefaultEventLoopGroup(2);
        b = new Bootstrap()
                // todo create I/O group and channel selection (may be through property but prefer native)
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                // defaults, can be overridden
                .remoteAddress("localhost", 9000)
                // todo make configurable, because on macosx there are no clashes with Nagle's Algorithm
                .option(TCP_NODELAY, true)
                .handler(new ChannelInitializer() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline()
                                // decoders
                                .addFirst(PACKET_DECODER, new ClickHousePacketDecoder())
                                .addLast(workersGroup, "messageHandler", ClickHouseServerMessageHandler.CLICK_HOUSE_SERVER_MESSAGE_HANDLER)
                                // encoders
                                .addFirst(PACKET_ENCODER, CLICK_HOUSE_PACKET_ENCODER)
                                .addFirst(BLOCK_ENCODER, DataBlockEncoder.DATA_BLOCK_ENCODER);
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

    public ClickHouseClient compression(CompressionMethod method) {
        this.settings.put(NETWORK_COMPRESSION_METHOD, new SettingCompressionMethod(method));
        return this;
    }

    public <T> Flow.Publisher<Void> sendData(String query, Flow.Publisher<T[]> source) {
        return this.sendData("", query, source);
    }

    /**
     * Reactive way for sending data.
     * Data object {@code T[]} collects from upstream and buffers for sending latter in huge blocks
     *
     * @param queryId CH query identifier
     * @param query   CH query description of inserted data
     * @param source  reactive data publisher
     * @param <T>     type of incoming object array
     * @return empty {@code <Void>} publisher that signals success or error after insert process ends
     */
    public <T> Flow.Publisher<Void> sendData(String queryId, String query, Flow.Publisher<T[]> source) {
        query += " FORMAT Native";
        AuthData authData = new AuthData(database, username, password);
        return new EmptyResponsePublisher<>(b.clone(), workersGroup, compressionGroup, authData, queryId, query, settings, limits, source);
    }

    @Override
    public void close() {
        Future<?> configShutdown = b.config().group().shutdownGracefully();
        Future<?> decompressingShutdown = workersGroup.shutdownGracefully();

        configShutdown.syncUninterruptibly();
        decompressingShutdown.syncUninterruptibly();
    }
}
