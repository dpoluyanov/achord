package io.achord.test.extensions.logger;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * @author Camelion
 * @since 06.03.2018
 */
public final class SetJdkLoggerToFine implements BeforeAllCallback {
    @Override
    public void beforeAll(ExtensionContext context) {
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.FINE);
        for (Handler h : rootLogger.getHandlers()) {
            h.setLevel(Level.FINE);
        }
    }
}
