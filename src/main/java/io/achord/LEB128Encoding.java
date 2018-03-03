package io.achord;

import io.netty.buffer.ByteBuf;

final class LEB128Encoding {

    private LEB128Encoding() {
        /* restricted */
    }

    /**
     * Writes a long value to the given buffer in LEB128 ZigZag encoded format
     *
     * @param buffer the buffer to write to
     * @param value  the value to write to the buffer
     */
    static void putLong(ByteBuf buffer, long value) {
        if (value >>> 7 == 0) {
            buffer.writeByte((byte) value);
        } else {
            buffer.writeByte((byte) ((value & 0x7F) | 0x80));
            if (value >>> 14 == 0) {
                buffer.writeByte((byte) (value >>> 7));
            } else {
                buffer.writeByte((byte) (value >>> 7 | 0x80));
                if (value >>> 21 == 0) {
                    buffer.writeByte((byte) (value >>> 14));
                } else {
                    buffer.writeByte((byte) (value >>> 14 | 0x80));
                    if (value >>> 28 == 0) {
                        buffer.writeByte((byte) (value >>> 21));
                    } else {
                        buffer.writeByte((byte) (value >>> 21 | 0x80));
                        if (value >>> 35 == 0) {
                            buffer.writeByte((byte) (value >>> 28));
                        } else {
                            buffer.writeByte((byte) (value >>> 28 | 0x80));
                            if (value >>> 42 == 0) {
                                buffer.writeByte((byte) (value >>> 35));
                            } else {
                                buffer.writeByte((byte) (value >>> 35 | 0x80));
                                if (value >>> 49 == 0) {
                                    buffer.writeByte((byte) (value >>> 42));
                                } else {
                                    buffer.writeByte((byte) (value >>> 42 | 0x80));
                                    if (value >>> 56 == 0) {
                                        buffer.writeByte((byte) (value >>> 49));
                                    } else {
                                        buffer.writeByte((byte) (value >>> 49 | 0x80));
                                        buffer.writeByte((byte) (value >>> 56));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Writes an int value to the given buffer in LEB128-64b9B ZigZag encoded format
     *
     * @param buffer the buffer to write to
     * @param value  the value to write to the buffer
     */
    static void putInt(ByteBuf buffer, int value) {
        if (value >>> 7 == 0) {
            buffer.writeByte(value);
        } else {
            buffer.writeByte((value & 0x7F) | 0x80);
            if (value >>> 14 == 0) {
                buffer.writeByte((value >>> 7));
            } else {
                buffer.writeByte(value >>> 7 | 0x80);
                if (value >>> 21 == 0) {
                    buffer.writeByte(value >>> 14);
                } else {
                    buffer.writeByte(value >>> 14 | 0x80);
                    if (value >>> 28 == 0) {
                        buffer.writeByte(value >>> 21);
                    } else {
                        buffer.writeByte(value >>> 21 | 0x80);
                        buffer.writeByte(value >>> 28);
                    }
                }
            }
        }
    }

    /**
     * Read an LEB128-64b9B ZigZag encoded long value from the given buffer
     *
     * @param buffer the buffer to read from
     * @return the value read from the buffer
     */
    static long getLong(ByteBuf buffer) {
        long v = buffer.readByte();
        long value = v & 0x7F;
        if ((v & 0x80) != 0) {
            v = buffer.readByte();
            value |= (v & 0x7F) << 7;
            if ((v & 0x80) != 0) {
                v = buffer.readByte();
                value |= (v & 0x7F) << 14;
                if ((v & 0x80) != 0) {
                    v = buffer.readByte();
                    value |= (v & 0x7F) << 21;
                    if ((v & 0x80) != 0) {
                        v = buffer.readByte();
                        value |= (v & 0x7F) << 28;
                        if ((v & 0x80) != 0) {
                            v = buffer.readByte();
                            value |= (v & 0x7F) << 35;
                            if ((v & 0x80) != 0) {
                                v = buffer.readByte();
                                value |= (v & 0x7F) << 42;
                                if ((v & 0x80) != 0) {
                                    v = buffer.readByte();
                                    value |= (v & 0x7F) << 49;
                                    if ((v & 0x80) != 0) {
                                        v = buffer.readByte();
                                        value |= v << 56;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return value;
    }

    /**
     * Read an LEB128-64b9B ZigZag encoded int value from the given buffer
     *
     * @param buffer the buffer to read from
     * @return the value read from the buffer
     */
    static int getInt(ByteBuf buffer) {
        int v = buffer.readByte();
        int value = v & 0x7F;
        if ((v & 0x80) != 0) {
            v = buffer.readByte();
            value |= (v & 0x7F) << 7;
            if ((v & 0x80) != 0) {
                v = buffer.readByte();
                value |= (v & 0x7F) << 14;
                if ((v & 0x80) != 0) {
                    v = buffer.readByte();
                    value |= (v & 0x7F) << 21;
                    if ((v & 0x80) != 0) {
                        v = buffer.readByte();
                        value |= (v & 0x7F) << 28;
                    }
                }
            }
        }
        return value;
    }

    public static void putByte(ByteBuf buffer, int value) {
        if (value >>> 7 == 0) {
            buffer.writeByte(value);
        } else {
            buffer.writeByte((value & 0x7F) | 0x80);
            buffer.writeByte(value >>> 7);
        }
    }
}