package jvguard;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * jv-guard - Logging
 * <p>
 * Lightweight internal logger for the jv-guard engine.
 * No external dependencies. INFO and WARN route to stdout,
 * ERROR routes to stderr.
 * <p>
 * Usage (at the top of any jv-guard class):
 * <p>
 *     private static final Logging.JvLogger log = Logging.get(MyClass.class);
 * <p>
 * Then anywhere in that class:
 * <p>
 *     log.info("registry sealed - 12 methods registered");
 *     log.warn("ORDER: 1.5 invalid, defaulted to 0.0", "OrderService.processChild");
 *     log.error("executor failed to start", "Executers.runningCpuExecutor");
 */
public final class Logging {

    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    // Static utility class - no instances
    private Logging() {}

    /**
     * Returns a named logger for the given class.
     * The class simple name is embedded in every log line.
     */
    public static JvLogger get(Class<?> source) {
        return new JvLogger(source.getSimpleName());
    }

    // -------------------------------------------------------------------------

    public static final class JvLogger {

        private final String name;

        private JvLogger(String name) {
            this.name = name;
        }

        // ---------------------------------------------------------------------
        // Core log methods
        // ---------------------------------------------------------------------

        /** General engine information. Seal confirmation, pool build, idle spin start. */
        public void info(String message) {
            emit("INFO", message, null, false);
        }

        /**
         * Non-fatal issue. Used for Tier 2 ORDER autocorrection.
         * Always include context when the source is a user-decorated method.
         */
        public void warn(String message) {
            emit("WARN", message, null, false);
        }

        /** Non-fatal issue with caller context - preferred for validator warnings. */
        public void warn(String message, String context) {
            emit("WARN", message, context, false);
        }

        /** Engine-level failure. Routes to stderr. */
        public void error(String message) {
            emit("ERROR", message, null, true);
        }

        /** Engine-level failure with caller context. Routes to stderr. */
        public void error(String message, String context) {
            emit("ERROR", message, context, true);
        }

        // ---------------------------------------------------------------------
        // Formatting
        // ---------------------------------------------------------------------

        private void emit(String level, String message, String context, boolean stderr) {
            String line = String.format(
                    "[jv-guard] [%s] [%s] %s%s",
                    level,
                    name,
                    context != null ? "[ " + context + "] " : "",
                    message
            );

            String timestamped = LocalTime.now().format(TIME_FMT) + "  " + line;

            if (stderr) {
                System.err.println(timestamped);
            } else {
                System.out.println(timestamped);
            }
        }
    }
}