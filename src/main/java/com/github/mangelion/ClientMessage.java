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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

/**
 * @author Camelion
 * @since 14/02/2018
 */
abstract class ClientMessage {
    static void writeVarUInt(ByteBuf buf, long value) {
        LEB128Encoding.putLong(buf, value);
    }

    static void writeStringBinary(ByteBuf buf, CharSequence charSequence) {
        writeVarUInt(buf, charSequence.length());
        ByteBufUtil.writeUtf8(buf, charSequence);
    }

    static void writeVarUInt(ByteBuf buf, int value) {
        LEB128Encoding.putInt(buf, value);
    }

    static void writeVarUInt(ByteBuf buf, boolean value) {
        LEB128Encoding.putByte(buf, value ? 1 : 0);
    }

    abstract ByteBuf createPayload(ByteBufAllocator alloc);
}
