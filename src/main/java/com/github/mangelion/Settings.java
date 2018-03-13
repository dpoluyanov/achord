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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.github.mangelion.ClientMessage.writeStringBinary;
import static com.github.mangelion.ClientMessage.writeVarUInt;

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

    void put(String key, Setting setting) {
        settings.put(key, setting);
    }

    static abstract class Setting {
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

        SettingCompressionMethod(CompressionMethod value) {
            this.value = Objects.requireNonNull(value);
        }

        @Override
        protected void writeTo(ByteBuf buf) {
            writeStringBinary(buf, value.toString().toLowerCase());
        }
    }
}
