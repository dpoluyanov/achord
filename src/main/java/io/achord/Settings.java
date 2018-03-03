package io.achord;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.achord.ClientMessage.writeStringBinary;
import static io.achord.ClientMessage.writeVarUInt;
import static java.util.Map.entry;

/**
 * @author Camelion
 * @since 25.12.2017
 */
final class Settings {
    static final String NETWORK_COMPRESSION_METHOD = "network_compression_method";
    static final String NETWORK_ZSTD_COMPRESSION_LEVEL = "network_zstd_compression_level";

    private final Map<String, Setting> settings = new HashMap<>();

    void write(ByteBuf buf) {
        for (Map.Entry<String, Setting> setting : settings.entrySet()) {
            writeStringBinary(buf, setting.getKey());
            setting.getValue().writeTo(buf);
        }
    }

    boolean isCompressionEnabled() {
        return settings.containsKey(NETWORK_COMPRESSION_METHOD);
    }

    CompressionMethod getNetworkCompressionMethod() {
        return ((SettingCompressionMethod) settings.getOrDefault(NETWORK_COMPRESSION_METHOD,
                new SettingCompressionMethod(CompressionMethod.LZ4)))
                .value;
    }

    long getNetworkZstdCompressionLevel() {
        return ((SettingInt64) settings.getOrDefault(NETWORK_ZSTD_COMPRESSION_LEVEL, new SettingInt64(1L)))
                .value;
    }

    static abstract class Setting {
        static Map.Entry<String, SettingCompressionMethod> networkCompressionMethod(CompressionMethod method) {
            return entry(NETWORK_COMPRESSION_METHOD, new SettingCompressionMethod(method));
        }

        static Map.Entry<String, SettingInt64> networkZstdCompressionLevel(long value) {
            return entry(NETWORK_ZSTD_COMPRESSION_LEVEL, new SettingInt64(value));
        }

        abstract void writeTo(ByteBuf buf);
    }

    static final class SettingInt64 extends Setting {

        final long value;

        SettingInt64(long value) {
            this.value = value;
        }

        @Override
        protected void writeTo(ByteBuf buf) {
            writeVarUInt(buf, value);
        }
    }

    static final class SettingCompressionMethod extends Setting {
        final CompressionMethod value;

        private SettingCompressionMethod(CompressionMethod value) {
            this.value = Objects.requireNonNull(value);
        }

        @Override
        protected void writeTo(ByteBuf buf) {
            writeStringBinary(buf, value.toString().toLowerCase());
        }
    }
}
