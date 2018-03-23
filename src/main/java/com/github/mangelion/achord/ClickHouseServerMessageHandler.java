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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author Dmitriy Poluyanov
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        QueryContext context = ctx.channel().attr(QueryContext.QUERY_CONTEXT_ATTR).get();
        context.onChannelExceptionCaught(cause);
        ctx.channel().close();
    }
}
