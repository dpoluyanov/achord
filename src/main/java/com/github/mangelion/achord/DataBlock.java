/*
 * Copyright 2017-2018 Mangelion
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mangelion.achord;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

/**
 * @author Dmitriy Poluyanov
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
