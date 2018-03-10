package com.github.mangelion;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

/**
 * @author Camelion
 * @since 19/02/2018
 */
final class CompressedBlock extends AbstractReferenceCounted {
    final int method;
    final ByteBuf compressed;
    final int decompressedSize;

    CompressedBlock(int method, ByteBuf compressed, int decompressedSize) {
        this.method = method;
        this.compressed = compressed;
        this.decompressedSize = decompressedSize;
    }

    @Override
    protected void deallocate() {
        compressed.release();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }
}
