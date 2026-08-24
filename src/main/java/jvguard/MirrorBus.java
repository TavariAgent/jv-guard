package jvguard;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * jv-guard - MirrorBus
 *
 * Runtime transfer layer for MIRROR chains.
 * Producers route typed results here after execution.
 * Consumers await their result before invoking.
 *
 * One LinkedBlockingQueue per consumer OP. Handles concurrent invocations
 * of the same chain naturally -- results queue and consumers drain in FIFO order.
 *
 * Consumer tasks MUST run on AEXC or IEXC (virtual threads). take() parks
 * the virtual thread and frees the carrier -- no OS thread is blocked.
 * CEXC consumers are a Tier 1 violation at seal() and never reach this path.
 */
public final class MirrorBus {

    private static final Logging.JvLogger log = Logging.get(MirrorBus.class);

    private static volatile MirrorBus instance;

    // One queue per consumer OP. Created on first access, reused across invocations.
    private final ConcurrentHashMap<String, LinkedBlockingQueue<Object>> channels
            = new ConcurrentHashMap<>();

    private MirrorBus() {}

    public static void initialize() {
        instance = new MirrorBus();
        log.info("mirror bus initialized");
    }

    public static MirrorBus get() {
        if (instance == null) {
            throw new IllegalStateException(
                    "jv-guard: MirrorBus not initialized - call MirrorBus.initialize() first"
            );
        }
        return instance;
    }

    // -------------------------------------------------------------------------
    // Producer side
    // -------------------------------------------------------------------------

    /**
     * Routes a result to the named consumer OP.
     * If a consumer is already waiting, it unparks immediately.
     * If no consumer is waiting yet, the result queues until one arrives.
     */
    public void route(String consumerOp, Object result) {
        channel(consumerOp).add(result);
    }

    // -------------------------------------------------------------------------
    // Consumer side
    // -------------------------------------------------------------------------

    /**
     * Awaits the next result for this OP. Parks the calling virtual thread
     * until a producer routes a result. Returns the typed-erased result --
     * the executor performs the seal-validated cast before invocation.
     *
     * Interruption propagates cleanly -- the executor's finally block
     * calls onComplete() regardless.
     */
    public Object await(String consumerOp) throws InterruptedException {
        return channel(consumerOp).take();
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private LinkedBlockingQueue<Object> channel(String op) {
        return channels.computeIfAbsent(op, k -> new LinkedBlockingQueue<>());
    }
}