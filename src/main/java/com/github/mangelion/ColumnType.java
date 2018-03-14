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
 * @since 14.03.2018
 */
final class ColumnType {
    private static final byte INT_8 = 0;
    private static final byte U_INT_8 = 1;
    private static final byte INT_32 = 2;
    private static final byte U_INT_32 = 3;
    private static final byte INT_64 = 4;
    private static final byte U_INT_64 = 5;
    private static final byte STRING = 6;
    private static final byte DATE = 7;
    private static final byte DATETIME = 8;

    static byte valueOf(String typeName) {
        switch (typeName) {
            case "Int8":
                return INT_8;
            case "UInt8":
                return U_INT_8;
            case "Int32":
                return INT_32;
            case "UInt32":
                return U_INT_32;
            case "Int64":
                return INT_64;
            case "UInt64":
                return U_INT_64;
            case "String":
                return STRING;
            case "Date":
                return DATE;
            case "DateTime":
                return DATETIME;
            default:
                throw new IllegalArgumentException("Can not find according type for name " + typeName);
        }
    }

    static CharSequence valueOf(byte type) {
        switch (type) {
            case INT_8:
                return "Int8";
            case U_INT_8:
                return "UInt8";
            case INT_32:
                return "Int32";
            case U_INT_32:
                return "UInt32";
            case INT_64:
                return "Int64";
            case U_INT_64:
                return "UInt64";
            case STRING:
                return "String";
            case DATE:
                return "Date";
            case DATETIME:
                return "DateTime";
            default:
                throw new IllegalArgumentException("Can not find according name for type " + type);
        }
    }

    static void read(byte type, ByteBuf from, ByteBuf to, int count) {
        switch (type) {
            case INT_8:
            case U_INT_8:
                from.readBytes(to, count);
                return;
            case INT_32:
            case U_INT_32:
            case DATETIME:
                from.readBytes(to, count * 4);
                return;
            case INT_64:
            case U_INT_64:
                from.readBytes(to, count * 8);
            case STRING:
                readString(from, to, count);
                return;
            case DATE:
                from.readBytes(to, count * 2);
                return;
            default:
                throw new IllegalArgumentException("Can not read unknown type " + type);
        }
    }

    private static void readString(ByteBuf in, ByteBuf out, int count) {
        int from = in.readerIndex();
        in.markReaderIndex();
        for (int i = 0; i < count; i++) {
            int strSize = (int) readVarUInt(in);

            in.readerIndex(in.readerIndex() + strSize);
        }

        int to = in.readerIndex();
        in.resetReaderIndex();

        in.readBytes(out, to - from);
    }

    static void write(byte type, Object val, ByteBuf buf) {
        switch (type) {
            case INT_8:
            case U_INT_8:
                buf.writeByte((byte) val);
                return;
            case INT_32:
            case U_INT_32:
                buf.writeIntLE((int) val);
                return;
            case INT_64:
            case U_INT_64:
                buf.writeLongLE((long) val);
                return;
            case STRING:
                writeStringBinary(buf, (String) val);
                return;
            // possible writeShortLe and writeIntLe below
            case DATE:
                Temporal date = (Temporal) val;
                buf.writeShort(date.get(EPOCH_DAY));
                return;
            case DATETIME:
                Temporal dateTime = (Temporal) val;
                buf.writeInt((int) dateTime.getLong(INSTANT_SECONDS));
                return;
            default:
                throw new IllegalArgumentException("Can not write unknown type " + type);
        }
    }
}