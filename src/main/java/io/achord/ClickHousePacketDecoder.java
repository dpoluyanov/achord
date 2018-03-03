package io.achord;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import java.util.List;

import static io.achord.ClickHousePacketDecoder.ServerProtocol.*;
import static io.achord.EndOfStreamMessage.END_OF_STREAM_MESSAGE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Camelion
 * @since 13/02/2018
 */
final class ClickHousePacketDecoder extends ReplayingDecoder<Void> {
    static final AttributeKey<CompressionMethod> CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE = AttributeKey.newInstance("CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE");
    static final AttributeKey<Long> CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE = AttributeKey.newInstance("CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE");
    private static final AttributeKey<Long> CH_SERVER_REVISION_ATTRIBUTE = AttributeKey.newInstance("CH_SERVER_REVISION");

    static long readVarUInt(ByteBuf buf) {
        return LEB128Encoding.getLong(buf);
    }

    static String readStringBinary(ByteBuf buf) {
        int length = (int) readVarUInt(buf);

        ByteBuf stringBuf = buf.readBytes(length);
        try {
            return stringBuf.toString(UTF_8);
        } finally {
            ReferenceCountUtil.release(stringBuf);
        }
    }

    static DataBlock readBlock(ChannelHandlerContext ctx, ByteBuf in) {
        BlockInfo info = new BlockInfo();
        info.read(in);

        int columns = (int) readVarUInt(in);
        int rows = (int) readVarUInt(in);

        ColumnWithTypeAndName[] cs = new ColumnWithTypeAndName[columns];
        for (int i = 0; i < columns; i++) {
            String columnName = readStringBinary(in);
            ColumnType type = ColumnType.valueOf(readStringBinary(in));

            ColumnWithTypeAndName column = cs[i] = new ColumnWithTypeAndName(type, columnName, ctx.alloc().directBuffer());

            if (rows > 0) {
                column.type.read(in, column, rows);
            }
        }

        return new DataBlock(info, cs, rows);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        byte msgId = (byte) readVarUInt(in);
        switch (msgId) {
            case HELLO_MSG_ID:
                // small message so most of time it not breaks onto different parts
                Object sm = readHelloMsg(in, ctx);
                out.add(sm);
                break;
            case DATA_MSG_ID:
                String external_table_name = readStringBinary(in);
                CompressionMethod method = ctx.channel().attr(CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE).get();

                if (method != null) {
                    long t1 = in.readLongLE();
                    long t2 = in.readLongLE();

                    byte compressionMethod = in.readByte();
                    int compressedSize = in.readIntLE();
                    int decompressedSize = in.readIntLE();

                    assert checkChecksum(t1, t2,
                            in.slice(in.readerIndex() - CompressionMethod.HEADER_SIZE, compressedSize),
                            compressedSize);

                    // because we know block size we can decode block in more efficient way in next handler (and on another event loop group)
                    ByteBuf compressed = in.readBytes(compressedSize - CompressionMethod.HEADER_SIZE);

                    CompressedBlock compressedBlock = new CompressedBlock(compressionMethod, compressed, decompressedSize);
                    out.add(compressedBlock);
                } else {
                    // inefficient reading attempts for uncompressed blocks
                    DataBlock dataBlock = readBlock(ctx, in);
                    out.add(dataBlock);
                }
                break;
            case EXCEPTION_MSG_ID:
                ClickHouseServerException e = readException(in);
                out.add(e);
                break;
            case END_OF_STREAM_MSG_ID:
                out.add(END_OF_STREAM_MESSAGE);
                break;
        }
    }

    private ClickHouseServerException readException(ByteBuf buf) {
        int code = buf.readInt();
        String name = readStringBinary(buf);
        String message = readStringBinary(buf);
        String exception = readStringBinary(buf);
        boolean has_nested = buf.readBoolean();

        ClickHouseServerException e = new ClickHouseServerException(code, name, message, exception);
        if (has_nested) {
            ClickHouseServerException nested = readException(buf);
            e.addSuppressed(nested);
        }

        return e;
    }

    private boolean checkChecksum(long t1, long t2, ByteBuf in, int length) {
        UInt128 hash = CityHash_v1_0_2.CityHash128(in, length);

        if (hash.first != t1 || hash.second != t2) {
            fail("Checksum does not match, corrupted data");
            // does not happens
            return false;
        }

        return true;
    }

    void fail(String message) {
        throw new DecoderException(message);
    }


    private ServerMessage readHelloMsg(ByteBuf in, ChannelHandlerContext ctx) {
        String serverName = readStringBinary(in);
        long serverVersionMajor = readVarUInt(in);
        long serverVersionMinor = readVarUInt(in);
        long serverRevision = readVarUInt(in);

        ctx.channel().attr(CH_SERVER_REVISION_ATTRIBUTE).setIfAbsent(serverRevision);

        if (serverRevision >= ServerRevisions.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
            String serverTimezone = readStringBinary(in);
            return new ClickHouseServerInfo(serverName, serverVersionMajor, serverVersionMinor, serverRevision, serverTimezone);
        }

        return new ClickHouseServerInfo(serverName, serverVersionMajor, serverVersionMinor, serverRevision);
    }

    static class ServerProtocol {
        static final byte HELLO_MSG_ID = 0x00;
        static final byte DATA_MSG_ID = 0x01;
        static final byte EXCEPTION_MSG_ID = 0x02;
        static final byte END_OF_STREAM_MSG_ID = 0x05;
    }
}
