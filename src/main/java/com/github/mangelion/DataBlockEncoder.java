package com.github.mangelion;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import static com.github.mangelion.ClientMessage.writeStringBinary;
import static com.github.mangelion.ClientMessage.writeVarUInt;

/**
 * @author Camelion
 * @since 19/02/2018
 */
@ChannelHandler.Sharable
final class DataBlockEncoder extends MessageToByteEncoder<DataBlock> {
    static final DataBlockEncoder DATA_BLOCK_ENCODER = new DataBlockEncoder();
    private static final int DATA_MSG_ID = 0x02;

    private DataBlockEncoder() { /* restricted */ }

    @Override
    protected void encode(ChannelHandlerContext ctx, DataBlock block, ByteBuf out) {
        writeHeader(block, out);
        writeBlock(block, out);
    }

    static void writeHeader(DataBlock block, ByteBuf out) {
        writeVarUInt(out, DATA_MSG_ID);
        writeStringBinary(out, ""); // Block name
    }

    static void writeBlock(DataBlock block, ByteBuf out) {
        block.info.write(out);

        ColumnWithTypeAndName[] columns = block.columns;

        writeVarUInt(out, columns.length);
        writeVarUInt(out, block.rows);

        for (int i = 0; i < columns.length; i++) {
            ColumnWithTypeAndName c = columns[i];
            writeStringBinary(out, c.name);
            writeStringBinary(out, c.type.name());
            if (block.rows > 0) {
                out.writeBytes(c.data);
            }
        }
    }
}
