package io.achord;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

/**
 * @author Camelion
 * @since 14/02/2018
 */
abstract class ClientMessage {
    static void writeVarUInt(ByteBuf buf, int value) {
        LEB128Encoding.putInt(buf, value);
    }

    static void writeVarUInt(ByteBuf buf, long value) {
        LEB128Encoding.putLong(buf, value);
    }

    static void writeStringBinary(ByteBuf buf, CharSequence charSequence) {
        writeVarUInt(buf, charSequence.length());
        ByteBufUtil.writeUtf8(buf, charSequence);
    }

    static void writeVarUInt(ByteBuf buf, boolean value) {
        LEB128Encoding.putByte(buf, value ? 1 : 0);
    }

    abstract ByteBuf createPayload(ByteBufAllocator alloc);
}
