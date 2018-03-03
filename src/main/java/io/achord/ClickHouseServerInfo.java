package io.achord;

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
