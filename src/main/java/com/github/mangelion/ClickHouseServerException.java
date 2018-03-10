package com.github.mangelion;

import io.netty.handler.codec.DecoderException;

/**
 * @author Camelion
 * @since 24.12.2017
 */
final class ClickHouseServerException extends DecoderException {
    ClickHouseServerException(int code, String name, String message, String exception) {
        super(makeMessage(code, name, message, exception));
    }

    private static String makeMessage(int code, String name, String message, String exception) {
        return "\n" + name + ", code [" + code + "], message: " + message;
        // verbose
        // + "\nServerException:\n" + exception;
    }
}
