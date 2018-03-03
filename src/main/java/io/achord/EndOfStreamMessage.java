package io.achord;

/**
 * @author Camelion
 * @since 26.12.2017
 */
final class EndOfStreamMessage implements ServerMessage {
    static final EndOfStreamMessage END_OF_STREAM_MESSAGE = new EndOfStreamMessage();

    private EndOfStreamMessage() { /* restricted */ }
}
