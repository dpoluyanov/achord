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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;

import static com.github.mangelion.achord.ClickHousePacketDecoder.CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE;
import static com.github.mangelion.achord.ClickHousePacketDecoder.CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE;

/**
 * @author Dmitriy Poluyanov
 * @since 21/02/2018
 */
@ChannelHandler.Sharable
final class BlockCompressingHandler extends MessageToByteEncoder<DataBlock> {
    static final BlockCompressingHandler BLOCK_COMPRESSING_HANDLER = new BlockCompressingHandler();

    @Override
    protected void encode(ChannelHandlerContext ctx, DataBlock msg, ByteBuf out) {
        CompressionMethod compressionMethod = ctx.channel().attr(CH_SERVER_COMPRESSION_METHOD_ATTRIBUTE).get();
        long level = ctx.channel().attr(CH_SERVER_COMPRESSION_LEVEL_ATTRIBUTE).get();

        DataBlockEncoder.writeHeader(msg, out);

        int inStart;
        ByteBuf in = null;
        try {
            in = ctx.alloc().buffer();
            inStart = in.writerIndex();
            DataBlockEncoder.writeBlock(msg, in);
            // todo: at this point we can switch compression method on-the-fly
            // depending on channel writeability (zstd or lzhc if not writable and vice versa lz4)
            compressionMethod.compress(in, inStart, out, level);
        } finally {
            ReferenceCountUtil.release(in);
        }
    }
}
