package io.achord;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.ReferenceCountUtil;

import java.util.List;

import static io.achord.ClickHousePacketDecoder.CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE;
import static io.achord.ClickHousePacketDecoder.CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE;
import static io.achord.DataBlockEncoder.writeBlock;

/**
 * @author Camelion
 * @since 21/02/2018
 */
@ChannelHandler.Sharable
final class BlockCompressingHandler extends MessageToMessageEncoder<DataBlock> {
    static final BlockCompressingHandler BLOCK_COMPRESSING_HANDLER = new BlockCompressingHandler();

    @Override
    protected void encode(ChannelHandlerContext ctx, DataBlock msg, List<Object> out) {
        CompressionMethod compressionMethod = ctx.channel().attr(CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE).get();
        long level = ctx.channel().attr(CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE).get();

        ByteBuf decompressed = ctx.alloc().directBuffer();
        try {
            writeBlock(msg, decompressed);
        } catch (Throwable e) {
            ReferenceCountUtil.release(decompressed);
            throw e;
        }

        ByteBuf compressed = compressionMethod.compress(decompressed, level);
        out.add(compressed);
    }
}
