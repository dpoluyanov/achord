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
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import static com.github.mangelion.ClickHouseClient.COMPATIBLE_CLIENT_REVISION;

/**
 * @author Camelion
 * @since 14/02/2018
 */
final class HelloMessage extends ClientMessage {
    private final static ByteBuf STATIC;
    private static final int HELLO_MSG_ID = 0x00;

    static {
        ByteBuf buf = null;
        try {
            buf = Unpooled.directBuffer();
            writeVarUInt(buf, HELLO_MSG_ID);
            writeStringBinary(buf, "AChord"); // Client name
            writeVarUInt(buf, 1); // major
            writeVarUInt(buf, 1); // minor
            writeVarUInt(buf, COMPATIBLE_CLIENT_REVISION); // build number

            STATIC = buf.copy(0, buf.writerIndex());
        } finally {
            ReferenceCountUtil.release(buf);
        }
    }

    private final AuthData authData;

    HelloMessage(AuthData authData) {
        this.authData = authData;
    }

    @Override
    ByteBuf createPayload(ByteBufAllocator alloc) {
        ByteBuf buf = alloc.directBuffer();
        writeStringBinary(buf, authData.database);
        writeStringBinary(buf, authData.username);
        writeStringBinary(buf, authData.password);

        return alloc.compositeDirectBuffer(2)
                .addComponents(true, STATIC.retainedSlice(), buf);
    }
}
