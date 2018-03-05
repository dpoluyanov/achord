package io.achord;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

/**
 * @author Camelion
 * @since 18/02/2018
 */
final class DataBlock extends AbstractReferenceCounted {
    static final DataBlock EMPTY = new DataBlock(new BlockInfo(), new ColumnWithTypeAndName[0], 0);
    // before usage should be retained
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
        for (int i = 0; i < columns.length; i++) {
            ReferenceCountUtil.release(columns[i].data);
            columns[i] = null;
        }
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }
}
