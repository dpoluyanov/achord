package io.achord;

/**
 * @author Camelion
 * @since 14/02/2018
 */
final class AuthData {
    final String database;
    final String username;
    final String password;

    AuthData(String database, String username, String password) {
        this.database = database;
        this.username = username;
        this.password = password;
    }
}
