package io.achord;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

/**
 * @author Camelion
 * @since 19/02/2018
 */
@ChannelHandler.Sharable
final class BlockDecompressingHandler extends MessageToMessageDecoder<CompressedBlock> {
    static final BlockDecompressingHandler BLOCK_DECOMPRESSING_HANDLER = new BlockDecompressingHandler();

    @Override
    protected void decode(ChannelHandlerContext ctx, CompressedBlock compressedBlock, List<Object> out) throws Exception {
        ByteBuf decompressed;
        switch (compressedBlock.method) {
            case 0x82:
                decompressed = CompressionMethod.LZ4.decompress(compressedBlock.compressed.retain(), compressedBlock.decompressedSize);
                break;
            case 0x90:
                decompressed = CompressionMethod.ZSTD.decompress(compressedBlock.compressed.retain(), compressedBlock.decompressedSize);
                break;
            case 0x02:
                decompressed = CompressionMethod.NONE.decompress(compressedBlock.compressed.retain(), compressedBlock.decompressedSize);
                break;
            default:
                throw new IllegalStateException("Unknown compression method [" + Integer.toHexString(compressedBlock.method) + "]");
        }

        DataBlock block = ClickHousePacketDecoder.readBlock(ctx, decompressed);
        out.add(block);
    }
}
