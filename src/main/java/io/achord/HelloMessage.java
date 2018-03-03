package io.achord;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import static io.achord.ClickHouseClient.COMPATIBLE_CLIENT_REVISION;

/**
 * @author Camelion
 * @since 14/02/2018
 */
final class HelloMessage extends ClientMessage {
    private final static ByteBuf STATIC;
    private static final int HELLO_MSG_ID = 0x00;

    static {
        ByteBuf buf = null;
        try {
            buf = Unpooled.directBuffer();
            writeVarUInt(buf, HELLO_MSG_ID);
            writeStringBinary(buf, "Achord"); // Client name
            writeVarUInt(buf, 1); // major
            writeVarUInt(buf, 1); // minor
            writeVarUInt(buf, COMPATIBLE_CLIENT_REVISION); // build number

            STATIC = buf.copy(0, buf.writerIndex());
        } finally {
            ReferenceCountUtil.release(buf);
        }
    }

    private final AuthData authData;

    HelloMessage(AuthData authData) {
        this.authData = authData;
    }

    @Override
    ByteBuf createPayload(ByteBufAllocator alloc) {
        ByteBuf buf = alloc.directBuffer();
        writeStringBinary(buf, authData.database);
        writeStringBinary(buf, authData.username);
        writeStringBinary(buf, authData.password);

        return alloc.compositeDirectBuffer(2)
                .addComponents(true, STATIC.slice().retain(), buf);
    }
}
