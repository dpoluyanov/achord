package com.github.mangelion;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author Camelion
 * @since 18/02/2018
 */
@ChannelHandler.Sharable
final class ClickHouseServerMessageHandler extends ChannelInboundHandlerAdapter {

    static final ClickHouseServerMessageHandler CLICK_HOUSE_SERVER_MESSAGE_HANDLER = new ClickHouseServerMessageHandler();

    private ClickHouseServerMessageHandler() { /* restricted */ }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DataBlock || msg instanceof ServerMessage || msg instanceof ClickHouseServerException) {
            QueryContext queryContext = ctx.channel().attr(QueryContext.QUERY_CONTEXT_ATTR).get();
            if (queryContext == null) {
                throw new RuntimeException("Query context is not set");
            }

            if (msg instanceof DataBlock) {
                DataBlock dataBlock = (DataBlock) msg;
                dataBlock.retain();
                queryContext.onDataBlockReceived(dataBlock);
            } else if (msg instanceof ClickHouseServerInfo) {
                queryContext.onClickHouseServerInfoReceived((ClickHouseServerInfo) msg);
            } else if (msg == EndOfStreamMessage.END_OF_STREAM_MESSAGE) {
                queryContext.onEndOfStream();
            } else if (msg instanceof ClickHouseServerException) {
                queryContext.onServerExceptionCaught((ClickHouseServerException) msg);
                ctx.channel().close();
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        QueryContext context = ctx.channel().attr(QueryContext.QUERY_CONTEXT_ATTR).get();
        context.onChannelExceptionCaught(cause);
        ctx.channel().close();
    }
}
