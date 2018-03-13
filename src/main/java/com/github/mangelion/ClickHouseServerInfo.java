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

/**
 * @author Camelion
 * @since 25.12.2017
 */
final class ClickHouseServerInfo implements ServerMessage {
    final long serverRevision;
    private final String serverName;
    private final long serverVersionMajor;
    private final long serverVersionMinor;
    private final String serverTimezone;

    ClickHouseServerInfo(String serverName, long serverVersionMajor, long serverVersionMinor, long serverRevision) {
        this(serverName, serverVersionMajor, serverVersionMinor, serverRevision, null);
    }

    ClickHouseServerInfo(String serverName, long serverVersionMajor, long serverVersionMinor, long serverRevision, String serverTimezone) {
        this.serverName = serverName;
        this.serverVersionMajor = serverVersionMajor;
        this.serverVersionMinor = serverVersionMinor;
        this.serverRevision = serverRevision;
        this.serverTimezone = serverTimezone;
    }
}
