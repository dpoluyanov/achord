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

import io.netty.util.AttributeKey;

/**
 * @author Dmitriy Poluyanov
 * @since 11/02/2018
 */
interface QueryContext {
    AttributeKey<QueryContext> QUERY_CONTEXT_ATTR = AttributeKey.valueOf("QUERY_CONTEXT");

    @Deprecated
    AuthData getAuthData();

    void onChannelConnected();

    void onClickHouseServerInfoReceived(ClickHouseServerInfo serverInfo);

    // because all block is reference counted, it should be released as soon as possible
    void onDataBlockReceived(DataBlock block);

    void onServerExceptionCaught(ClickHouseServerException exception);

    void onEndOfStream();

    void onChannelExceptionCaught(Throwable cause);
}
