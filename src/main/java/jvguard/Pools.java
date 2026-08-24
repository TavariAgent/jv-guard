package jvguard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * jv-guard - Pools
 *
 * CoreRegistry construction, per-pool queue management, LEAD suffix allocation,
 * and idle worker spin loops. Called once after Wrapping.seal().
 *
 * Startup sequence:
 *
 *     PoolConfig config = new PoolConfig.Builder()
 *         .iPool(2)
 *         .pPool(2)
 *         .hPool(2)
 *         .rPool(0)            // rPool grows dynamically from iPool at runtime
 *         .workersPerCore(4)   // matches c-guard baseline
 *         .build();
 *
 *     CoreRegistry registry = Pools.build(config, dispatcher);
 *
 * The TaskDispatcher (provided by Handler) receives a DispatchPacket when a
 * worker pulls a task. DispatchPacket carries the task plus any LEAD suffix
 * context needed for release - no external tracking required.
 *
 * Pool affinity is behavioural: idle workers spin continuously via
 * Thread.onSpinWait() (CPU PAUSE hint on x86). A thread that never blocks
 * gives the OS no preemption trigger - the core stays warm and cache locality
 * becomes a strong tendency. This is intentional; it matches how HFT systems
 * achieve soft affinity without native pinning calls.
 */
public final class Pools {

    private static final Logging.JvLogger log = Logging.get(Pools.class);

    public static final String I_POOL = "I";
    public static final String P_POOL = "P";
    public static final String H_POOL = "H";
    public static final String R_POOL = "R";

    private static volatile CoreRegistry activeRegistry = null;

    private Pools() {}

    // -------------------------------------------------------------------------
    // Build
    // -------------------------------------------------------------------------

    /**
     * Builds the CoreRegistry, assigns cores to pools, spawns idle workers,
     * and starts them spinning. Returns the sealed registry for Handler to hold.
     *
     * If total requested cores exceed availableProcessors(), the build caps
     * to available and logs a warning - partial assignment, not a failure.
     */
    public static CoreRegistry build(PoolConfig config, TaskDispatcher dispatcher) {
        int available = Runtime.getRuntime().availableProcessors();
        int requested = config.totalCores();

        if (requested > available) {
            log.warn("requested " + requested + " cores but only "
                    + available + " logical core(s) available - capping");
        }

        List<CoreEntry>        cores      = new ArrayList<>();
        Map<String, PoolQueue> poolQueues = new ConcurrentHashMap<>();
        List<Thread>           workers    = new ArrayList<>();

        // One PoolQueue per pool type - shared across all cores of that type
        for (String pool : new String[]{ I_POOL, P_POOL, H_POOL, R_POOL }) {
            poolQueues.put(pool, new PoolQueue(pool));
        }

        // Assign core IDs sequentially across pool types
        int nextId = 0;
        nextId = assignCores(cores, workers, poolQueues, I_POOL,
                config.iPoolCores(), config.workersPerCore(), dispatcher, nextId, available);
        nextId = assignCores(cores, workers, poolQueues, P_POOL,
                config.pPoolCores(), config.workersPerCore(), dispatcher, nextId, available);
        nextId = assignCores(cores, workers, poolQueues, H_POOL,
                config.hPoolCores(), config.workersPerCore(), dispatcher, nextId, available);
        nextId = assignCores(cores, workers, poolQueues, R_POOL,
                config.rPoolCores(), config.workersPerCore(), dispatcher, nextId, available);

        CoreRegistry registry = new CoreRegistry(
                Collections.unmodifiableList(cores),
                Collections.unmodifiableMap(poolQueues),
                Collections.unmodifiableList(workers)
        );

        activeRegistry = registry;

        // All workers are daemon threads - they do not prevent JVM shutdown
        for (Thread worker : workers) {
            worker.setDaemon(true);
            worker.start();
        }

        log.info("pool registry built - "
                + cores.size()   + " core(s), "
                + workers.size() + " worker(s) spinning");

        return registry;
    }

    /** Returns the active registry. Throws if called before build(). */
    public static CoreRegistry getRegistry() {
        if (activeRegistry == null) {
            throw new IllegalStateException(
                    "jv-guard: pool registry not built - call Pools.build() first"
            );
        }
        return activeRegistry;
    }

    // -------------------------------------------------------------------------
    // Internal - core assignment
    // -------------------------------------------------------------------------

    private static int assignCores(
            List<CoreEntry>        cores,
            List<Thread>           workers,
            Map<String, PoolQueue> poolQueues,
            String                 poolType,
            int                    count,
            int                    workersPerCore,
            TaskDispatcher         dispatcher,
            int                    startId,
            int                    cap
    ) {
        if (count == 0) return startId;

        PoolQueue queue = poolQueues.get(poolType);
        int       id    = startId;

        for (int i = 0; i < count && id < cap; i++, id++) {
            cores.add(new CoreEntry(id, poolType));

            for (int w = 0; w < workersPerCore; w++) {
                final String name = "jvg-" + poolType.toLowerCase() + "-" + id + "-w" + w;
                Thread worker = new Thread(() -> idleWorker(queue, dispatcher, name));
                worker.setName(name);
                workers.add(worker);
            }

            log.info("core " + id + " - " + poolType
                    + "Pool (" + workersPerCore + " worker(s))");
        }

        return id;
    }

    // -------------------------------------------------------------------------
    // Idle worker spin loop
    // -------------------------------------------------------------------------

    /**
     * Spin loop for a single worker thread.
     *
     * Priority order on each spin:
     *   1. Back segment (LEAD pinnacle tasks) - always checked first.
     *      pollBack() returns an EnrichedLeadTask carrying both the task
     *      and its suffix. Both are packed into the DispatchPacket.
     *   2. Front segment (regular GROUP/ORDER tasks) - checked if back is empty.
     *      PriorityBlockingQueue ensures workers always receive the lowest
     *      available (GROUP, ORDER) task regardless of insertion order.
     *   3. Thread.onSpinWait() - emitted when both segments are empty.
     *      Emits CPU PAUSE hint (x86): reduces power draw and prevents
     *      memory order violations in tight spin loops. The thread never
     *      releases the core - this is what creates behavioural pool affinity.
     */
    private static void idleWorker(
            PoolQueue      queue,
            TaskDispatcher dispatcher,
            String         name
    ) {
        while (!Thread.currentThread().isInterrupted()) {

            // Back first - LEAD pinnacle work, highest priority
            // EnrichedLeadTask carries task + suffix together
            PoolQueue.EnrichedLeadTask leadTask = queue.pollBack();
            if (leadTask != null) {
                dispatcher.dispatch(new DispatchPacket(
                        leadTask.task(),
                        leadTask.suffix(),
                        queue
                ));
                continue;
            }

            // Front - regular GROUP/ORDER tasks
            // PriorityBlockingQueue.poll() returns lowest (GROUP, ORDER) or null
            Wrapping.TaskDescriptor task = queue.pollFront();
            if (task != null) {
                dispatcher.dispatch(new DispatchPacket(task, -1, null));
                continue;
            }

            // Both segments empty - spin with CPU pause hint
            Thread.onSpinWait();
        }

        log.info(name + " interrupted - exiting spin loop");
    }

    // -------------------------------------------------------------------------
    // TaskDispatcher
    // -------------------------------------------------------------------------

    /**
     * Functional interface for handing a ready task to Handler.
     * Implemented by Handler and provided to Pools.build() at startup.
     * Workers call dispatch() with a DispatchPacket when a task is pulled.
     * Keeps Pools decoupled from Handler and Executers.
     */
    @FunctionalInterface
    public interface TaskDispatcher {
        void dispatch(DispatchPacket packet);
    }

    // -------------------------------------------------------------------------
    // DispatchPacket
    // -------------------------------------------------------------------------

    /**
     * Thin envelope passed from idle workers to Handler via TaskDispatcher.
     * Carries the task alongside LEAD suffix context so Handler can release
     * the suffix after execution without any external tracking map.
     *
     * LEAD tasks:     leadSuffix >= 1, sourceQueue non-null
     * Regular tasks:  leadSuffix == -1, sourceQueue null
     */
    public record DispatchPacket(
            Wrapping.TaskDescriptor task,
            int                     leadSuffix,
            PoolQueue               sourceQueue
    ) {
        public boolean isLead() { return leadSuffix >= 1; }
    }

    // -------------------------------------------------------------------------
    // CoreRegistry
    // -------------------------------------------------------------------------

    /**
     * Immutable registry of all active cores, pool queues, and worker threads.
     * Built once by Pools.build() and never modified after that.
     */
    public static final class CoreRegistry {

        private final List<CoreEntry>        cores;
        private final Map<String, PoolQueue> poolQueues;
        private final List<Thread>           workers;

        private CoreRegistry(
                List<CoreEntry>        cores,
                Map<String, PoolQueue> poolQueues,
                List<Thread>           workers
        ) {
            this.cores      = cores;
            this.poolQueues = poolQueues;
            this.workers    = workers;
        }

        public List<CoreEntry>        cores()      { return cores; }
        public Map<String, PoolQueue> poolQueues() { return poolQueues; }
        public List<Thread>           workers()    { return workers; }

        /**
         * Returns the PoolQueue for a given pool type.
         * Called by Handler.submit() when routing a task by its POOL tag.
         */
        public PoolQueue queueFor(String poolType) {
            PoolQueue q = poolQueues.get(poolType);
            if (q == null) {
                throw new IllegalArgumentException(
                        "jv-guard: no queue for pool type '" + poolType + "'"
                );
            }
            return q;
        }

        /**
         * Interrupts all worker threads. Workers exit their spin loops cleanly
         * on the next iteration. Call during application shutdown.
         */
        public void shutdown() {
            workers.forEach(Thread::interrupt);
            log.info("all pool workers interrupted - shutdown signalled");
        }
    }

    // -------------------------------------------------------------------------
    // CoreEntry
    // -------------------------------------------------------------------------

    /** One logical core and the pool type it is assigned to. */
    public record CoreEntry(int coreId, String poolType) {}

    // -------------------------------------------------------------------------
    // PoolQueue
    // -------------------------------------------------------------------------

    /**
     * Per-pool shared queue. Shared across all cores of the same pool type.
     * All workers in a pool read from the same instance.
     *
     * Two segments:
     *
     *   Front  - PriorityBlockingQueue<TaskDescriptor>
     *     Regular tasks. Ordered by (GROUP ascending, ORDER ascending).
     *     Workers always receive the lowest available (GROUP, ORDER) task
     *     regardless of concurrent insertion order. poll() is non-blocking.
     *
     *   Back   - ConcurrentSkipListMap<LeadKey, EnrichedLeadTask>
     *     LEAD pinnacle tasks. Sorted by (lead integer, promotedOrder).
     *     Lower LEAD integer = higher priority. LEAD: 0 always first.
     *     Each entry stores the task and its suffix together (EnrichedLeadTask)
     *     so the DispatchPacket can carry both without external tracking.
     *
     * LEAD suffix allocation:
     *
     *   Each LEAD integer has a free list (ConcurrentLinkedDeque<Integer>).
     *   When LEAD: 0 arrives and free list is empty -> counter increments -> suffix 1.
     *   When another LEAD: 0 arrives -> suffix 2. First completes -> releases _1.
     *   Next LEAD: 0 arrival -> reclaims _1 (lowest recycled first).
     *
     *   Promoted sort key = suffix (whole number) + ORDER (fractional part).
     *   Ranges [1.0,2.0), [2.0,3.0) ... never overlap - collision-free.
     */
    public static final class PoolQueue {

        private final String poolType;

        // Front segment - regular tasks, auto-ordered by (GROUP, ORDER)
        private final PriorityBlockingQueue<Wrapping.TaskDescriptor> front =
                new PriorityBlockingQueue<>(16_384, Comparator
                        .comparingInt(Wrapping.TaskDescriptor::group)
                        .thenComparingDouble(Wrapping.TaskDescriptor::order));

        // Back segment - LEAD pinnacle tasks, sorted by LeadKey
        // Stores EnrichedLeadTask so suffix travels with the task
        private final ConcurrentSkipListMap<LeadKey, EnrichedLeadTask> back =
                new ConcurrentSkipListMap<>();

        // Per-LEAD free lists - recycled suffix integers, lowest first
        private final ConcurrentHashMap<Integer, ConcurrentLinkedDeque<Integer>> leadFreeLists =
                new ConcurrentHashMap<>();

        // Per-LEAD high-water counters - expand when free list is empty
        private final ConcurrentHashMap<Integer, AtomicInteger> leadCounters =
                new ConcurrentHashMap<>();

        PoolQueue(String poolType) {
            this.poolType = poolType;
        }

        // --- Front ------------------------------------------------------------

        /** Deposits a regular task into the front priority queue. */
        public void offerFront(Wrapping.TaskDescriptor task) {
            front.offer(task);
        }

        /**
         * Polls the highest-priority regular task (lowest GROUP, lowest ORDER).
         * Returns null if the front queue is empty. Non-blocking.
         */
        public Wrapping.TaskDescriptor pollFront() {
            return front.poll();
        }

        // --- Back (LEAD) ------------------------------------------------------

        /**
         * Claims a suffix for the task's LEAD integer, packs task + suffix into
         * an EnrichedLeadTask, computes the promoted sort key, and inserts into
         * the back skip list. The suffix travels with the task from here forward -
         * no external tracking required.
         */
        public void offerBack(Wrapping.TaskDescriptor task) {
            int    suffix      = claimSuffix(task.lead());
            double promotedKey = suffix + task.order();
            back.put(new LeadKey(task.lead(), promotedKey),
                    new EnrichedLeadTask(task, suffix));
        }

        /**
         * Polls the highest-priority LEAD entry (lowest LeadKey).
         * Returns an EnrichedLeadTask (task + suffix) or null if empty.
         */
        public EnrichedLeadTask pollBack() {
            Map.Entry<LeadKey, EnrichedLeadTask> entry = back.pollFirstEntry();
            return entry != null ? entry.getValue() : null;
        }

        public boolean backIsEmpty() { return back.isEmpty(); }

        // --- LEAD suffix management -------------------------------------------

        /**
         * Claims the lowest available suffix for a LEAD integer.
         * Recycles from the free list first. Expands the counter if empty.
         */
        public int claimSuffix(int leadId) {
            ConcurrentLinkedDeque<Integer> freeList =
                    leadFreeLists.computeIfAbsent(leadId, k -> new ConcurrentLinkedDeque<>());

            Integer recycled = freeList.pollFirst();
            if (recycled != null) return recycled;

            return leadCounters
                    .computeIfAbsent(leadId, k -> new AtomicInteger(0))
                    .incrementAndGet();
        }

        /**
         * Returns a suffix to the free list after a LEAD task completes.
         * Called by Handler via the onComplete callback - always in a finally block.
         * A leaked suffix permanently inflates the counter without benefit.
         */
        public void releaseSuffix(int leadId, int suffix) {
            ConcurrentLinkedDeque<Integer> freeList = leadFreeLists.get(leadId);
            if (freeList != null) {
                freeList.addFirst(suffix); // lowest suffix recycles first
            }
        }

        // --- EnrichedLeadTask -------------------------------------------------

        /**
         * Wraps a LEAD task with its claimed suffix for storage in the back skip list.
         * Ensures the suffix travels with the task through pollBack() into the
         * DispatchPacket - eliminating the need for an external suffix tracking map.
         */
        public record EnrichedLeadTask(Wrapping.TaskDescriptor task, int suffix) {}
    }

    // -------------------------------------------------------------------------
    // LeadKey
    // -------------------------------------------------------------------------

    /**
     * Composite sort key for the back-segment ConcurrentSkipListMap.
     *
     * Sort order:
     *   1. lead integer - lower value = higher pinnacle priority.
     *      LEAD: 0 always before LEAD: 1, LEAD: 2, etc.
     *   2. promotedOrder - within the same LEAD, lower promoted order runs first.
     *      promotedOrder = suffix (whole number) + ORDER decimal.
     *
     * Collision guarantee: suffixes are unique non-overlapping integers per LEAD
     * and ORDER is validated to [0.0, 1.0). The promoted ranges [1.0,2.0),
     * [2.0,3.0) ... never overlap. Two tasks can never produce an identical
     * LeadKey through normal usage.
     */
    public record LeadKey(int lead, double promotedOrder) implements Comparable<LeadKey> {

        @Override
        public int compareTo(LeadKey other) {
            int byLead = Integer.compare(this.lead, other.lead);
            if (byLead != 0) return byLead;
            return Double.compare(this.promotedOrder, other.promotedOrder);
        }
    }

    // -------------------------------------------------------------------------
    // PoolConfig
    // -------------------------------------------------------------------------

    /**
     * User-facing pool configuration. Build with the inner Builder.
     *
     *     PoolConfig config = new PoolConfig.Builder()
     *         .iPool(2)
     *         .pPool(2)
     *         .hPool(2)
     *         .rPool(0)           // rPool starts empty; grows from iPool at runtime
     *         .workersPerCore(4)  // default 4
     *         .build();
     *
     * Total cores should not exceed Runtime.availableProcessors().
     * Over-assignment is capped with a warning, not an error.
     */
    public static final class PoolConfig {

        private final int iPoolCores;
        private final int pPoolCores;
        private final int hPoolCores;
        private final int rPoolCores;
        private final int workersPerCore;

        private PoolConfig(Builder b) {
            this.iPoolCores     = b.iPoolCores;
            this.pPoolCores     = b.pPoolCores;
            this.hPoolCores     = b.hPoolCores;
            this.rPoolCores     = b.rPoolCores;
            this.workersPerCore = b.workersPerCore;
        }

        public int iPoolCores()     { return iPoolCores; }
        public int pPoolCores()     { return pPoolCores; }
        public int hPoolCores()     { return hPoolCores; }
        public int rPoolCores()     { return rPoolCores; }
        public int workersPerCore() { return workersPerCore; }
        public int totalCores()     { return iPoolCores + pPoolCores + hPoolCores + rPoolCores; }

        public static final class Builder {

            private int iPoolCores     = 0;
            private int pPoolCores     = 0;
            private int hPoolCores     = 0;
            private int rPoolCores     = 0;
            private int workersPerCore = 4;

            public Builder iPool(int cores)         { this.iPoolCores     = cores; return this; }
            public Builder pPool(int cores)         { this.pPoolCores     = cores; return this; }
            public Builder hPool(int cores)         { this.hPoolCores     = cores; return this; }
            public Builder rPool(int cores)         { this.rPoolCores     = cores; return this; }
            public Builder workersPerCore(int count) { this.workersPerCore = count; return this; }

            public PoolConfig build() {
                if (iPoolCores < 0 || pPoolCores < 0 || hPoolCores < 0 || rPoolCores < 0) {
                    throw new IllegalArgumentException(
                            "jv-guard: pool core counts must be >= 0"
                    );
                }
                if (workersPerCore < 1) {
                    throw new IllegalArgumentException(
                            "jv-guard: workersPerCore must be >= 1"
                    );
                }
                return new PoolConfig(this);
            }
        }
    }
}