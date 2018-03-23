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
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * @author Dmitriy Poluyanov
 * @since 28.12.2017
 * todo make compression engines dynamically loadable, and exclude them from classpath
 */
public enum CompressionMethod {
    LZ4(1, 0x82) {
        @Override
        void compress(ByteBuf in, int inStart, ByteBuf out, long level) {
            int uncompressedSize = in.writerIndex() - inStart;
            int maxBound = lz4CompressBound(uncompressedSize);
            int outStart = out.writerIndex();
            int compressedDataPos = outStart + HASH_SIZE + HEADER_SIZE;

            // ensure writable
            out.ensureWritable(compressedDataPos + maxBound)
                    .writerIndex(compressedDataPos + maxBound);

            // use fastest possible instance for compression (and java-instance should be used for decompression)
            LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
            LZ4Compressor lz4Compressor = lz4Factory.fastCompressor();

            int compressedSize = HEADER_SIZE + lz4Compressor.compress(in.nioBuffer(),
                    inStart,
                    uncompressedSize, out.nioBuffer(), compressedDataPos, maxBound);
            out.writerIndex(compressedDataPos + compressedSize - HEADER_SIZE);

            out.setByte(outStart + HASH_SIZE + 0, getMethodByte())
                    .setIntLE(outStart + HASH_SIZE + 1, compressedSize)
                    .setIntLE(outStart + HASH_SIZE + 5, uncompressedSize);

            UInt128 hash = CityHash_v1_0_2.CityHash128(out.slice(outStart + HASH_SIZE, compressedSize), compressedSize);

            out.setLongLE(outStart, hash.first)
                    .setLongLE(outStart + 8, hash.second);
        }

        @Override
        ByteBuf decompress(ByteBuf input, int decompressedSize, ByteBufAllocator alloc) {
            ByteBuf dest = null;
            try {
                LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
                LZ4FastDecompressor lz4FastDecompressor = lz4Factory.fastDecompressor();

                dest = alloc.buffer(decompressedSize, decompressedSize)
                        .writerIndex(decompressedSize);

                if (lz4FastDecompressor.decompress(input.nioBuffer(), input.readerIndex(),
                        dest.nioBuffer(), 0, decompressedSize) < 0) {
                    throw new RuntimeException("Cannot decompress with LZ4");
                }
                return dest;
            } catch (Throwable e) {
                ReferenceCountUtil.release(dest);
                throw e;
            }
        }
    },
    LZ4HC(2, 0x82) {
        @Override
        void compress(ByteBuf in, int inStart, ByteBuf out, long level) {
        }

        @Override
        ByteBuf decompress(ByteBuf input, int decompressedSize, ByteBufAllocator alloc) {
            return null;
        }
    },

    ZSTD(3, 0x90) {
        @Override
        void compress(ByteBuf in, int inStart, ByteBuf out, long level) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ByteBuf decompress(ByteBuf input, int decompressedSize, ByteBufAllocator alloc) {
            return null;
        }
    },

    NONE(4, 0x02) {
        @Override
        void compress(ByteBuf in, int inStart, ByteBuf out, long level) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ByteBuf decompress(ByteBuf input, int decompressedSize, ByteBufAllocator alloc) {
            return null;
        }
    };

    public static final int HASH_SIZE = 16;
    static final int HEADER_SIZE = 9;
    static final int LZ4_MAX_INPUT_SIZE = 0x7E000000;

    private final int id;
    private final int methodByte;

    CompressionMethod(int id, int methodByte) {
        this.id = id;
        this.methodByte = methodByte;
    }

    static int lz4CompressBound(long isize) {
        return (isize) > LZ4_MAX_INPUT_SIZE ? 0 : (int) ((isize) + ((isize) / 255) + 16);
    }

    public int getId() {
        return id;
    }

    abstract void compress(ByteBuf input, int inStart, ByteBuf output, long level);

    abstract ByteBuf decompress(ByteBuf input, int decompressedSize, ByteBufAllocator alloc);

    public int getMethodByte() {
        return methodByte;
    }
}
