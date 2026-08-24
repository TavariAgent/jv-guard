package jvguard;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * jv-guard - Executers
 *
 * Three running executors, each handling a specific work category:
 *
 *   runningCpuExecutor   - CEXC - fixed OS thread pool, CPU-bound work
 *   runningIoExecutor    - IEXC - virtual thread per task, IO-bound work
 *   runningAsyncExecutor - AEXC - virtual thread per task, async work
 *
 * Invocation path:
 *   If TaskDescriptor.cachedHandle() is non-null, invokeExact() is called
 *   directly -- no reflection overhead. After JIT warmup this becomes an
 *   inlined direct call. Tasks with a null handle fall back to Method.invoke().
 *   Both paths throw Throwable; catch blocks handle both uniformly.
 *
 * Startup (after Handler.initialize()):
 *
 *     Executers.start();
 *
 * Shutdown (before JVM exit):
 *
 *     Executers.shutdown();
 *     Pools.getRegistry().shutdown();
 */
public final class Executers {

    private static final Logging.JvLogger log = Logging.get(Executers.class);

    private static volatile ExecutorService runningCpuExecutor   = null;
    private static volatile ExecutorService runningIoExecutor    = null;
    private static volatile ExecutorService runningAsyncExecutor = null;

    private Executers() {}

    // -------------------------------------------------------------------------
    // Start
    // -------------------------------------------------------------------------

    /**
     * Builds all three executors and registers their routes with Handler.
     * Must be called after Handler.initialize().
     *
     * runningCpuExecutor:
     *   Fixed OS thread pool sized to availableProcessors().
     *   OS threads hold the core -- correct for CPU-bound work that must not yield.
     *
     * runningIoExecutor / runningAsyncExecutor:
     *   Virtual thread per task (Project Loom, JDK 21+).
     *   IO blocks park the virtual thread and yield the carrier -- the carrier
     *   picks up the next virtual thread immediately. Near-zero cost per blocked
     *   thread. Two separate instances so each can be shut down independently.
     */
    public static void start() {
        Handler handler  = Handler.get();
        int     cpuCores = Runtime.getRuntime().availableProcessors();

        runningCpuExecutor = Executors.newFixedThreadPool(
                cpuCores,
                Thread.ofPlatform().name("jvg-cpu-", 0).factory()
        );

        runningIoExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("jvg-io-", 0).factory()
        );

        runningAsyncExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("jvg-async-", 0).factory()
        );

        handler.registerRoute("CEXC", (task, onComplete) ->
                runningCpuExecutor.submit(() -> {
                    try {
                        if (task.isMirrorProducer()) invokeProducer(task);
                        else                         invokeVoid(task);
                    } catch (Throwable e) {
                        log.error("task threw: " + e.getMessage(), task.context());
                    } finally {
                        onComplete.run();
                    }
                })
        );

        handler.registerRoute("IEXC", (task, onComplete) ->
                runningIoExecutor.submit(() -> {
                    try {
                        if      (task.isMirrorProducer() && task.isMirrorConsumer()) invokeMidChain(task);
                        else if (task.isMirrorProducer()) invokeProducer(task);
                        else if (task.isMirrorConsumer()) invokeConsumer(task);
                        else                              invokeVoid(task);
                    } catch (Throwable e) {
                        log.error("task threw: " + e.getMessage(), task.context());
                    } finally {
                        onComplete.run();
                    }
                })
        );

        handler.registerRoute("AEXC", (task, onComplete) ->
                runningAsyncExecutor.submit(() -> {
                    try {
                        if      (task.isMirrorProducer() && task.isMirrorConsumer()) invokeMidChain(task);
                        else if (task.isMirrorProducer()) invokeProducer(task);
                        else if (task.isMirrorConsumer()) invokeConsumer(task);
                        else                              invokeVoid(task);
                    } catch (Throwable e) {
                        log.error("task threw: " + e.getMessage(), task.context());
                    } finally {
                        onComplete.run();
                    }
                })
        );

        log.info("executors started - "
                + "CEXC: " + cpuCores + " OS thread(s)  "
                + "IEXC: virtual  "
                + "AEXC: virtual");
    }

    /** Original path -- no-arg void handle or reflection. Unchanged. */
    private static void invokeVoid(Wrapping.TaskDescriptor task) throws Throwable {
        MethodHandle handle = task.cachedHandle();
        if (handle != null) {
            handle.invokeExact();
        } else {
            try {
                task.method().invoke(task.target());
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                throw (cause != null) ? cause : e;
            }
        }
    }

    /**
     * Producer path -- captures return value via rawHandle, routes to MirrorBus,
     * then auto-submits the consumer task. Normal void completion follows in finally.
     */
    private static void invokeProducer(Wrapping.TaskDescriptor task) throws Throwable {
        Object result = task.rawHandle().invoke();

        if (result != null) {
            MirrorBus.get().route(task.mirror(), result);
            // Auto-submit consumer -- same pattern as leadWork submitting leadHelper
            Handler.get().submit(task.mirror());
        } else {
            log.warn("MIRROR producer returned null - consumer not submitted", task.context());
        }
    }

    /**
     * Consumer path -- awaits typed result from MirrorBus (parks virtual thread),
     * performs seal-validated cast, invokes with result as first argument.
     */
    private static void invokeConsumer(Wrapping.TaskDescriptor task) throws Throwable {
        Object result = MirrorBus.get().await(task.op());

        // Seal-validated cast -- isAssignableFrom was proven at seal() time
        // isInstance check here catches runtime surprises (generics erasure edge cases)
        if (!task.mirrorInputType().isInstance(result)) {
            log.error("MIRROR type conflict: expected "
                    + task.mirrorInputType().getSimpleName()
                    + " but received "
                    + result.getClass().getSimpleName()
                    + " -- consumer dropped", task.context());
            return;
        }

        Object cast = task.mirrorInputType().cast(result);
        task.method().invoke(task.target(), cast);
    }

    // -------------------------------------------------------------------------
    // Shutdown
    // -------------------------------------------------------------------------

    /**
     * Shuts down all three executors. Waits up to 10 seconds for in-flight
     * tasks to complete, then forces shutdown.
     */
    public static void shutdown() {
        drain("CEXC",  runningCpuExecutor);
        drain("IEXC",  runningIoExecutor);
        drain("AEXC",  runningAsyncExecutor);
    }

    private static void drain(String name, ExecutorService exec) {
        if (exec == null || exec.isShutdown()) return;
        exec.shutdown();
        try {
            if (!exec.awaitTermination(10, TimeUnit.SECONDS)) {
                exec.shutdownNow();
                log.warn(name + " executor did not drain cleanly - forced shutdown");
            } else {
                log.info(name + " executor shut down cleanly");
            }
        } catch (InterruptedException e) {
            exec.shutdownNow();
            Thread.currentThread().interrupt();
            log.warn(name + " executor shutdown interrupted");
        }
    }

    // -------------------------------------------------------------------------
    // Invocation
    // -------------------------------------------------------------------------

    /**
     * Mid-chain path -- node is both a consumer (receives from bus) and a producer
     * (forwards return value to the next step). Awaits input, invokes with it,
     * routes the output. rawHandle signature: (InputType)ReturnType after bindTo().
     */
    private static void invokeMidChain(Wrapping.TaskDescriptor task) throws Throwable {
        Object result = MirrorBus.get().await(task.op());

        if (!task.mirrorInputType().isInstance(result)) {
            log.error("MIRROR type conflict at mid-chain node: expected "
                    + task.mirrorInputType().getSimpleName()
                    + " but received "
                    + result.getClass().getSimpleName()
                    + " -- chain halted", task.context());
            return;
        }

        Object cast   = task.mirrorInputType().cast(result);
        Object output = task.rawHandle().invoke(cast);

        if (output != null) {
            MirrorBus.get().route(task.mirror(), output);
            Handler.get().submit(task.mirror());
        } else {
            log.warn("MIRROR mid-chain returned null - chain halted", task.context());
        }
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public static ExecutorService cpuExecutor()   { return runningCpuExecutor;   }
    public static ExecutorService ioExecutor()    { return runningIoExecutor;     }
    public static ExecutorService asyncExecutor() { return runningAsyncExecutor;  }
}