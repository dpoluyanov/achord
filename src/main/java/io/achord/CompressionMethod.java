package io.achord;

import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

import static io.netty.buffer.Unpooled.*;

/**
 * @author Camelion
 * @since 28.12.2017
 * todo make compression engines dynamically loadable, and exclude them from classpath
 */
public enum CompressionMethod {
    LZ4(1, 0x82) {
        @Override
        ByteBuf compress(ByteBuf input, long level) {
            try {
                int uncompressedSize = input.readableBytes();
                int maxBound = HEADER_SIZE + lz4CompressBound(uncompressedSize);
                ByteBuf compressed = Unpooled.directBuffer(maxBound);
                compressed.writeByte(getMethodByte())
                        .writerIndex(maxBound);

                // use fastest possible instance for compression (and java-instance should be used for decompression)
                LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
                LZ4Compressor lz4Compressor = lz4Factory.fastCompressor();

                int compressedSize = HEADER_SIZE + lz4Compressor.compress(input.nioBuffer(),
                        input.readerIndex(),
                        input.readableBytes(), compressed.nioBuffer(), HEADER_SIZE, maxBound - HEADER_SIZE);

                compressed.setIntLE(1, compressedSize); // compressed size
                compressed.setIntLE(5, uncompressedSize);
                compressed.writerIndex(compressedSize);

                UInt128 hash = CityHash_v1_0_2.CityHash128(compressed, compressedSize);
                return wrappedBuffer(
                        buffer(16)
                                .writeLongLE(hash.first)
                                .writeLongLE(hash.second),
                        compressed);
            } finally {
                ReferenceCountUtil.release(input);
            }
        }

        @Override
        ByteBuf decompress(ByteBuf input, int decompressedSize) {
            try {
                LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
                LZ4FastDecompressor lz4FastDecompressor = lz4Factory.fastDecompressor();

                ByteBuf dest = Unpooled.directBuffer(decompressedSize, decompressedSize)
                        .writerIndex(decompressedSize);

                if (lz4FastDecompressor.decompress(input.nioBuffer(), input.readerIndex(),
                        dest.nioBuffer(), 0, decompressedSize) < 0) {
                    throw new RuntimeException("Cannot decompress with LZ4");
                }

                return dest;
            } finally {
                ReferenceCountUtil.release(input);
            }
        }
    },
    LZ4HC(2, 0x82) {
        @Override
        ByteBuf compress(ByteBuf input, long level) {
            try {
                int uncompressedSize = input.readableBytes();
                ByteBuf compressed = Unpooled.directBuffer(HEADER_SIZE + lz4CompressBound(uncompressedSize));
                compressed.writeByte(getMethodByte());

                // use fastest possible instance for compression (and java -instance should be used for decompression)
                LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
                LZ4Compressor lz4Compressor = lz4Factory.highCompressor();

                ByteBuffer dest;
                lz4Compressor.compress(input.nioBuffer(),
                        input.readerIndex(),
                        input.readableBytes(), dest = compressed.nioBuffer(), HEADER_SIZE, LZ4_MAX_INPUT_SIZE);

                compressed.setIntLE(1, dest.position()); // compressed size
                compressed.setIntLE(5, uncompressedSize);
                compressed.writerIndex(dest.position());

                UInt128 hash = CityHash_v1_0_2.CityHash128(input, input.readableBytes());
                return wrappedBuffer(
                        buffer(16)
                                .writeLongLE(hash.first)
                                .writeLongLE(hash.second),
                        compressed);
            } finally {
                ReferenceCountUtil.release(input);
            }
        }

        @Override
        ByteBuf decompress(ByteBuf input, int decompressedSize) {
            return null;
        }
    },

    ZSTD(3, 0x90) {
        @Override
        ByteBuf compress(ByteBuf input, long level) {
            try {
                int uncompressedSize = input.readableBytes();
                ByteBuf compressed = Unpooled.directBuffer((int) (HEADER_SIZE + Zstd.compressBound(uncompressedSize)));
                compressed.writeByte(getMethodByte());

                ByteBuffer dest;

                Zstd.compressDirectByteBuffer((dest = compressed.nioBuffer()), HEADER_SIZE, dest.limit() - HEADER_SIZE,
                        input.nioBuffer(), input.readerIndex(), input.readableBytes(), (int) level);

                compressed.setIntLE(1, dest.position()); // compressed size
                compressed.setIntLE(5, uncompressedSize);
                compressed.writerIndex(dest.position());

                UInt128 hash = CityHash_v1_0_2.CityHash128(input, input.readableBytes());
                return wrappedBuffer(
                        buffer(16)
                                .writeLongLE(hash.first)
                                .writeLongLE(hash.second),
                        compressed);
            } finally {
                ReferenceCountUtil.release(input);
            }
        }

        @Override
        ByteBuf decompress(ByteBuf input, int decompressedSize) {
            return null;
        }
    },

    NONE(4, 0x02) {
        @Override
        ByteBuf compress(ByteBuf input, long level) {
            ByteBuf header = buffer(9)
                    .writeByte(getMethodByte())
                    .writeIntLE(HEADER_SIZE + input.readableBytes())
                    .writeIntLE(input.readableBytes());

            ByteBuf compressed = wrappedBuffer(header, input);

            UInt128 hash = CityHash_v1_0_2.CityHash128(compressed, compressed.readableBytes());
            return compositeBuffer(2)
                    .addComponents(
                            buffer(16)
                                    .writeLongLE(hash.first)
                                    .writeLongLE(hash.second), compressed);

        }

        @Override
        ByteBuf decompress(ByteBuf input, int decompressedSize) {
            return null;
        }
    };

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

    abstract ByteBuf compress(ByteBuf input, long level);

    abstract ByteBuf decompress(ByteBuf input, int decompressedSize);

    public int getMethodByte() {
        return methodByte;
    }
}
