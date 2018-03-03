package io.achord;

import io.netty.buffer.ByteBuf;

/**
 * @author Camelion
 * @since 24.12.2017
 */
final class ColumnWithTypeAndName {
    final ColumnType type;
    final ByteBuf data;
    final String name;

    // todo may be lazy allocation with predefined size would be more effective
    ColumnWithTypeAndName(ColumnType type, String name, ByteBuf data) {
        this.type = type;
        this.name = name;
        this.data = data;
    }
}
