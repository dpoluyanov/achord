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

package com.github.mangelion;

import io.netty.buffer.ByteBuf;

import java.time.temporal.Temporal;

import static com.github.mangelion.ClickHousePacketDecoder.readVarUInt;
import static com.github.mangelion.ClientMessage.writeStringBinary;
import static java.time.temporal.ChronoField.EPOCH_DAY;
import static java.time.temporal.ChronoField.INSTANT_SECONDS;

/**
 * @author Camelion
 * @since 25.12.2017
 * <p>
 * Need to be completely redesign, because casting objects is so expensive
 */
enum ColumnType {
    Int8 {
        @Override
        void write(ByteBuf buf, Object val) {
            buf.writeByte((byte) val);
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            buf.readBytes(column.data, rows);
        }
    },
    UInt8 {
        @Override
        void write(ByteBuf buf, Object val) {
            buf.writeByte((byte) val);
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            buf.readBytes(column.data, rows);
        }
    },
    Int32 {
        @Override
        void write(ByteBuf buf, Object val) {
            buf.writeIntLE((int) val);
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            buf.readBytes(column.data, rows * 4);
        }
    },
    UInt32 {
        @Override
        void write(ByteBuf buf, Object val) {
            buf.writeIntLE((int) val);
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            buf.readBytes(column.data, rows * 4);
        }
    },
    Int64 {
        @Override
        void write(ByteBuf buf, Object val) {
            buf.writeLongLE((long) val);
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            buf.readBytes(column.data, rows * 8);
        }
    },
    UInt64 {
        @Override
        void write(ByteBuf buf, Object val) {
            buf.writeLongLE((long) val);
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            buf.readBytes(column.data, rows * 8);
        }
    },
    String {
        @Override
        void write(ByteBuf buf, Object val) {
            String str = (java.lang.String) val;
            writeStringBinary(buf, str);
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            int from = buf.readerIndex();
            buf.markReaderIndex();
            for (int i = 0; i < rows; i++) {
                int strSize = (int) readVarUInt(buf);

                buf.readerIndex(buf.readerIndex() + strSize);
            }

            int to = buf.readerIndex();
            buf.resetReaderIndex();

            buf.readBytes(column.data, to - from);
        }
    },
    Date {
        @Override
        void write(ByteBuf buf, Object val) {
            Temporal dateTime = (Temporal) val;
            buf.writeShort(dateTime.get(EPOCH_DAY));
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            buf.readBytes(column.data, rows * 2);
        }
    },
    DateTime {
        @Override
        void write(ByteBuf buf, Object val) {
            Temporal dateTime = (Temporal) val;
            buf.writeInt((int) dateTime.getLong(INSTANT_SECONDS));
        }

        @Override
        void read(ByteBuf buf, ColumnWithTypeAndName column, int rows) {
            buf.readBytes(column.data, rows * 4);
        }
    };

    abstract void write(ByteBuf buf, Object val);

    abstract void read(ByteBuf buf, ColumnWithTypeAndName column, int rows);
}
