package jvguard;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * jv-guard - @Task
 *
 * Decorates methods for registration with the jv-guard threading engine.
 * All decorated methods are validated at Wrapping.seal() before any
 * executor starts. No task runs until the registry passes clean.
 *
 * Required:  POOL, EXECUTER, OP
 * Optional:  GROUP, ORDER, LEAD, CACHE
 *
 * Validation tiers:
 *   Tier 1 - POOL, EXECUTER, OP, GROUP, LEAD
 *            Hard block. All violations reported together, thrown once.
 *
 *   Tier 2 - ORDER
 *            LEAD >= 0 + invalid ORDER  -> hard block (arithmetic promotion breaks)
 *            LEAD == -1 + invalid ORDER -> silently defaulted to 0.0, warning logged
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Task {

    // -------------------------------------------------------------------------
    // Required fields - no default, compiler enforces presence
    // -------------------------------------------------------------------------

    /**
     * Pool assignment. Routes this task to the correct core group.
     *
     *   "I"  iPool - idempotent work
     *   "P"  pPool - protocol-based work
     *   "H"  hPool - harmonic, static difficulty work
     *   "R"  rPool - command processing, hot-split from iPool cores
     */
    String POOL();

    /**
     * Executor target. Determines which running executor handles this task.
     *
     *   "CEXC"  runningCpuExecutor   - CPU-bound work
     *   "IEXC"  runningIoExecutor    - IO-bound work
     *   "AEXC"  runningAsyncExecutor - async work
     */
    String EXECUTER();

    /**
     * Task label. Used for routing, logging, and identification.
     * Must be non-blank. Example: "buildMultiJson"
     */
    String OP();

    /**
     * MIRROR target OP. When set, this task is a producer in a MIRROR chain.
     * The engine captures the return value and routes it to the named consumer
     * after normal void completion. The consumer must accept the producer's
     * return type as its first parameter. Consumer EXECUTER must be AEXC or
     * IEXC — CEXC blocks an OS thread awaiting the result, which is a Tier 1
     * violation at seal().
     *
     * Chain declaration:
     *   Task(..., MIRROR = "processResult")
     *   public String buildResult() { return "value"; }
     *
     *   Task(POOL = "P", EXECUTER = "AEXC", OP = "processResult")
     *   public void processResult(String value) { ... }
     *
     * Default: "" (no mirror)
     */
    String MIRROR() default "";

    // -------------------------------------------------------------------------
    // Optional fields - ordering and affinity
    // -------------------------------------------------------------------------

    /**
     * Phase group. Tasks are accepted from pool queues in ascending group order.
     * Lower integers run first. All tasks in a group are dispatched together
     * before the next group begins.
     *
     * Must be >= 0. Default: 0
     */
    int GROUP() default 0;

    /**
     * Float order within the group. Defines position within the current
     * GROUP phase. Must be in [0.0, 1.0) - exclusive upper bound.
     *
     * On LEAD tasks (LEAD >= 0):
     *   Promoted by the LEAD suffix integer as a whole number sum.
     *   Example: LEAD _1 + ORDER 0.003 -> sort key 1.003
     *   Invalid ORDER on a LEAD task is a hard block at seal().
     *
     * On non-LEAD tasks (LEAD == -1):
     *   Invalid ORDER is silently defaulted to 0.0 with a warning logged.
     *
     * Default: 0.0
     */
    double ORDER() default 0.0;

    /**
     * LEAD pinnacle rank. Defines back-of-queue priority for high-locality
     * or heavy work that should be hoisted ahead of standard GROUP phases.
     *
     *   -1        No affinity. Task routes to the front of the pool deque,
     *             ordered by GROUP then ORDER. Default behavior.
     *
     *   >= 0      Pinnacle. Task routes to the back segment (skip list),
     *             sorted by LEAD integer first (lower = higher priority),
     *             then by promoted ORDER key within the same LEAD.
     *             LEAD: 0 is always the highest pinnacle.
     *
     * When multiple tasks share the same LEAD integer simultaneously,
     * each receives a dynamic suffix (_1, _2, ...) allocated from a
     * per-LEAD free list. Suffix integers are promoted to whole numbers
     * and summed with ORDER to form the skip list sort key.
     * Suffixes are released on task return and recycled for future use.
     *
     * Children of a LEAD task must carry at least ORDER: 0.0 (the default).
     *
     * Default: -1 (no affinity)
     */
    int LEAD() default -1;

    /**
     * Pre-compiles a MethodHandle at seal() time for use on the hot invocation path.
     *
     * When true, a no-arg MethodHandle pre-bound to the target instance is built
     * during seal() and stored in the TaskDescriptor. Executers call invokeExact()
     * on it directly instead of going through reflection on every invocation.
     * After JIT warmup, invokeExact() is inlined to a direct call.
     *
     * The handle is only built if the method was successfully made accessible
     * during registration (trySetAccessible() returned true). If it was not,
     * a warning is logged at registration time and the engine falls back to
     * reflection automatically -- no action required.
     *
     * Set false to disable caching and always use reflection. Useful for
     * debugging invocation issues or for methods that cannot be made accessible.
     *
     * Default: true
     */
    boolean CACHE() default true;
}