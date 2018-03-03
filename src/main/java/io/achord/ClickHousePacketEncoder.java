package io.achord;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;

/**
 * @author Camelion
 * @since 15/02/2018
 */
@ChannelHandler.Sharable
final class ClickHousePacketEncoder extends MessageToByteEncoder<ClientMessage> {
    static final ClickHousePacketEncoder CLICK_HOUSE_PACKET_ENCODER = new ClickHousePacketEncoder();

    private ClickHousePacketEncoder() { /* restricted */ }

    @Override
    protected void encode(ChannelHandlerContext ctx, ClientMessage msg, ByteBuf out) {
        ByteBuf payload = null;
        try {
            payload = msg.createPayload(ctx.alloc());
            out.writeBytes(payload);
        } finally {
            ReferenceCountUtil.release(payload);
        }
    }
}
