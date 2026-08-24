package jvguard;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * jv-guard - Handler
 *
 * Task routing, phase coordination, and executor handoff.
 * The central dispatch layer sitting between pool queues and executors.
 *
 * Startup sequence (called after Wrapping.seal()):
 *
 *     Handler handler = Handler.initialize(poolConfig);
 *
 *     // Executers register their routes immediately after:
 *     handler.registerRoute("CEXC", (task, onComplete) -> cpuPool.submit(...));
 *     handler.registerRoute("IEXC", (task, onComplete) -> ioPool.submit(...));
 *     handler.registerRoute("AEXC", (task, onComplete) -> asyncPool.submit(...));
 *
 * Task submission (user-facing, called at runtime):
 *
 *     Handler.get().submit(taskDescriptor);
 *
 * Dependency note: Handler imports Pools but NOT Executers.
 * Executers import Handler and register themselves via registerRoute().
 * The dependency runs one way - no circular imports.
 */
public final class Handler {

    private static final Logging.JvLogger log = Logging.get(Handler.class);

    private static volatile Handler instance = null;

    private final Pools.CoreRegistry         registry;
    private final PhaseCoordinator           coordinator;
    private final Map<String, ExecutorRoute> executorRoutes;

    private Handler(Pools.CoreRegistry registry, int workerCount) {
        this.registry       = registry;
        this.coordinator    = new PhaseCoordinator(workerCount);
        this.executorRoutes = new ConcurrentHashMap<>();
    }

    // -------------------------------------------------------------------------
    // Initialization
    // -------------------------------------------------------------------------

    /**
     * Builds the pool registry, starts all idle workers, and returns the Handler.
     * Must be called after Wrapping.seal() and before any Executers start.
     *
     * Uses a forward-reference pattern: the TaskDispatcher lambda captures a
     * one-element array that is populated immediately after the registry builds.
     * Workers only begin dispatching after they start (post-build), so the
     * reference is always live before any dispatch call arrives.
     */
    public static Handler initialize(Pools.PoolConfig config) {
        if (instance != null) {
            throw new IllegalStateException(
                    "jv-guard: Handler already initialized"
            );
        }

        Handler[] ref = new Handler[1];

        Pools.TaskDispatcher dispatcher = packet -> {
            if (ref[0] != null) ref[0].onPull(packet);
        };

        Pools.CoreRegistry registry = Pools.build(config, dispatcher);
        Handler            handler  = new Handler(registry, registry.workers().size());
        ref[0]   = handler;
        instance = handler;

        log.info("handler initialized - "
                + registry.cores().size()   + " core(s), "
                + registry.workers().size() + " worker(s)");

        return handler;
    }

    /**
     * Returns the active Handler instance.
     * Throws if called before initialize().
     */
    public static Handler get() {
        if (instance == null) {
            throw new IllegalStateException(
                    "jv-guard: Handler not initialized - call Handler.initialize() first"
            );
        }
        return instance;
    }

    // -------------------------------------------------------------------------
    // Executor registration
    // -------------------------------------------------------------------------

    /**
     * Registers an executor route for a given EXECUTER tag ("CEXC", "IEXC", "AEXC").
     * Called by Executers.java after its thread pools are built.
     *
     * CONTRACT: the onComplete Runnable provided to each route MUST be called
     * inside a finally block. It handles LEAD suffix release and Phaser signalling.
     * Failing to call it leaks the suffix permanently and silently stalls
     * the phase counter.
     *
     *     handler.registerRoute("CEXC", (task, onComplete) ->
     *         cpuPool.submit(() -> {
     *             try   { task.method().invoke(target); }
     *             catch (Exception e) { log.error(...); }
     *             finally { onComplete.run(); }           // <- non-negotiable
     *         })
     *     );
     */
    public void registerRoute(String executer, ExecutorRoute route) {
        executorRoutes.put(executer, route);
        log.info("executor route registered - " + executer);
    }

    // -------------------------------------------------------------------------
    // Task submission (public API)
    // -------------------------------------------------------------------------

    /**
     * Submits a task by OP label. O(1) lookup via the sealed OP index.
     * Primary user-facing submit path - no descriptor reference required.
     *
     *     Handler.get().submit("buildMultiJson");
     */
    public void submit(String op) {
        Wrapping.TaskDescriptor task = Wrapping.getByOp(op);
        if (task == null) {
            log.error("no task registered with OP: '" + op + "' - dropped", "Handler");
            return;
        }
        submit(task);
    }

    /**
     * Submits a descriptor directly. Use submit(String op) at the call site
     * unless you already hold the descriptor.
     *
     * LEAD >= 0  → back skip list (offerBack).
     *              Suffix embedded in the entry - travels with the task automatically.
     * LEAD == -1 → front priority queue (offerFront).
     *              Ordered by (GROUP, ORDER) at insertion time.
     */
    public void submit(Wrapping.TaskDescriptor task) {
        Pools.PoolQueue queue = registry.queueFor(task.pool());

        if (task.lead() >= 0) {
            queue.offerBack(task);
        } else {
            queue.offerFront(task);
        }
    }

    // -------------------------------------------------------------------------
    // Internal dispatch - called by idle workers via TaskDispatcher
    // -------------------------------------------------------------------------

    /**
     * Receives a DispatchPacket from an idle worker and routes it to execution.
     *
     * Flow:
     *   1. Record phase acceptance for this task's GROUP/ORDER step.
     *   2. Build onComplete - releases LEAD suffix (if applicable) and
     *      signals Phaser arrival. Always runs via executor's finally block.
     *   3. Lookup registered executor route, hand off task + callback.
     */
    private void onPull(Pools.DispatchPacket packet) {
        Wrapping.TaskDescriptor task = packet.task();

        coordinator.onAccept(task);

        Runnable onComplete = () -> {
            // Release LEAD suffix directly from packet context - no tracking map needed
            if (packet.isLead()) {
                packet.sourceQueue().releaseSuffix(
                        task.lead(),
                        packet.leadSuffix()
                );
            }
            coordinator.onComplete(task);
        };

        ExecutorRoute route = executorRoutes.get(task.executer());

        if (route == null) {
            log.error(
                    "no executor route registered for '"
                            + task.executer() + "' - task dropped",
                    task.context()
            );
            onComplete.run(); // still release suffix and arrive at phaser on drop
            return;
        }

        route.execute(task, onComplete);
    }

    // -------------------------------------------------------------------------
    // ExecutorRoute
    // -------------------------------------------------------------------------

    /**
     * Functional interface for routing a ready task to an executor.
     * Implemented in Executers.java, registered via Handler.registerRoute().
     *
     * The onComplete Runnable must be called in a finally block inside the
     * submitted work. See registerRoute() contract above.
     */
    @FunctionalInterface
    public interface ExecutorRoute {
        void execute(Wrapping.TaskDescriptor task, Runnable onComplete);
    }

    // -------------------------------------------------------------------------
    // PhaseCoordinator
    // -------------------------------------------------------------------------

    /**
     * Tracks GROUP/ORDER phase steps and manages Phaser lifecycle.
     *
     * Phase model:
     *   Tasks are accepted from pool queues in ascending GROUP order.
     *   Within a GROUP, tasks are ordered by ascending ORDER value.
     *   The front PriorityQueue in Pools enforces this ordering at poll time -
     *   workers always receive the lowest available (GROUP, ORDER) task.
     *
     *   The Phaser is sized to the total worker count and is ready for two modes:
     *
     *   Mode 1 (current) - non-blocking tracking:
     *     onComplete() calls phaser.arrive() without waiting. This counts
     *     completions and advances the phase counter without blocking workers.
     *     GROUP/ORDER ordering is maintained by the priority queue alone.
     *
     *   Mode 2 (activation-ready) - hard step barriers:
     *     Replace phaser.arrive() with phaser.arriveAndAwaitAdvance() in
     *     onComplete(). All workers then synchronize at each ORDER step before
     *     advancing - "simultaneous acceptance" in the strict sense. No other
     *     code changes required; the Phaser is already sized correctly.
     *
     *   GROUP-level barriers (GROUP N fully drains before GROUP N+1 opens):
     *     Layer on top of Mode 2 by detecting GROUP transitions in onAccept()
     *     and using phaser.bulkRegister() / phaser.arriveAndDeregister() to
     *     adjust party counts at GROUP boundaries.
     */
    static final class PhaseCoordinator {

        private final Phaser                  phaser;
        private final AtomicInteger           currentGroup;
        private final AtomicReference<Double> currentOrder;

        PhaseCoordinator(int workerCount) {
            this.phaser       = new Phaser(workerCount);
            this.currentGroup = new AtomicInteger(0);
            this.currentOrder = new AtomicReference<>(0.0);
        }

        /**
         * Records that a worker has accepted a task at this GROUP/ORDER step.
         * Updates the step tracker for monitoring and future barrier gating.
         */
        void onAccept(Wrapping.TaskDescriptor task) {
            currentGroup.set(task.group());
            currentOrder.set(task.order());
        }

        /**
         * Signals that a task has completed execution.
         *
         * Mode 1 (current): non-blocking arrival.
         * Mode 2 (ready):   replace arrive() with arriveAndAwaitAdvance()
         *                   to activate hard step synchronisation.
         */
        void onComplete(Wrapping.TaskDescriptor task) {
            phaser.arrive();
        }

        int    currentGroup() { return currentGroup.get(); }
        double currentOrder() { return currentOrder.get(); }
    }
}