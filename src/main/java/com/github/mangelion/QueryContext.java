package com.github.mangelion;

import io.netty.util.AttributeKey;

/**
 * @author Camelion
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
