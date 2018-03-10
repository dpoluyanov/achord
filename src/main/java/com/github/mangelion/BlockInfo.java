package com.github.mangelion;

import io.netty.buffer.ByteBuf;

import static com.github.mangelion.ClickHousePacketDecoder.readVarUInt;
import static com.github.mangelion.ClientMessage.writeVarUInt;

/**
 * @author Camelion
 * @since 25.12.2017
 */
final class BlockInfo {
    private boolean is_overflows = false;
    private int bucket_num = -1;

    void write(ByteBuf buf) {
        // first field_num is is_overflows
        writeVarUInt(buf, 1);
        buf.writeBoolean(is_overflows);

        // second field_num is bucket_num,
        writeVarUInt(buf, 2);
        buf.writeInt(bucket_num);

        writeVarUInt(buf, 0);
    }

    void read(ByteBuf buf) {
        int field_num = (int) readVarUInt(buf);

        assert field_num == 1;

        is_overflows = buf.readBoolean();

        field_num = (int) readVarUInt(buf);

        assert field_num == 2;

        bucket_num = buf.readInt();

        field_num = (int) readVarUInt(buf);

        assert field_num == 0;
    }
}
