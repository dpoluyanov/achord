package io.achord;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

/**
 * @author Camelion
 * @since 18/02/2018
 */
final class DataBlock extends AbstractReferenceCounted {
    static final DataBlock EMPTY = new DataBlock(new BlockInfo(), new ColumnWithTypeAndName[0], 0);
    final BlockInfo info;
    final ColumnWithTypeAndName[] columns;
    final int rows;

    DataBlock(BlockInfo info, ColumnWithTypeAndName[] columns, int rows) {
        this.info = info;
        this.columns = columns;
        this.rows = rows;
    }

    @Override
    protected void deallocate() {
        for (ColumnWithTypeAndName column : columns) {
            column.data.release();
        }
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }
}
