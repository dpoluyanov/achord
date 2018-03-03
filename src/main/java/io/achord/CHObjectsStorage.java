package io.achord;

import io.netty.buffer.ByteBuf;

/**
 * @author Camelion
 * @since 11/02/2018
 */
public interface CHObjectsStorage {
    ByteBuf getData(int column);
}