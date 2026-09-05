package jvguard;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.WrongMethodTypeException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * jv-guard - Wrapping
 * <p>
 * Registration, validation, and seal for the jv-guard engine.
 * No executor starts before seal() passes clean.
 * <p>
 * Startup sequence:
 * <p>
 *     Wrapping.register(new OrderService());    // instance methods
 *     Wrapping.registerStatic(UtilTasks.class); // static methods
 *     Wrapping.seal();                          // validate + lock
 * <p>
 * After seal(), tasks are accessible by OP name for submission:
 * <p>
 *     Handler.get().submit("buildMultiJson");
 * <p>
 * MethodHandle caching:
 *   For every @Task(CACHE = true) method (the default), seal() pre-builds
 *   a no-arg MethodHandle bound to the target instance. Executers call
 *   invokeExact() on it directly -- after JIT warmup this is a direct call,
 *   not a reflection lookup. Tasks where handle construction fails fall back
 *   to reflection automatically with a warning logged at registration time.
 */
public final class Wrapping {

    private static final Logging.JvLogger log = Logging.get(Wrapping.class);

    // Valid field sets - source of truth for Tier 1 validation
    private static final Set<String> VALID_POOLS     = Set.of("I", "P", "H", "R");
    private static final Set<String> VALID_EXECUTERS = Set.of("CEXC", "IEXC", "AEXC");

    private static final List<TaskDescriptor> pending = new ArrayList<>();
    private static final AtomicBoolean        sealed  = new AtomicBoolean(false);

    private static volatile List<TaskDescriptor>        sealedRegistry = null;
    private static volatile Map<String, TaskDescriptor> opIndex        = null;

    private Wrapping() {}

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Scans an object instance for @Task annotated methods.
     * The instance is stored as the invocation target in each TaskDescriptor.
     * Primary registration method - use this for instance methods.
     */
    public static void register(Object instance) {
        scanClass(instance.getClass(), instance);
    }

    /**
     * Scans a class for @Task annotated static methods.
     * Invocation target is null - Method.invoke(null) is the correct Java
     * call for static methods and works without an instance.
     */
    public static void registerStatic(Class<?> clazz) {
        scanClass(clazz, null);
    }

    // -------------------------------------------------------------------------
    // Internal scan
    // -------------------------------------------------------------------------

    private static void scanClass(Class<?> clazz, Object target) {
        if (sealed.get()) {
            throw new IllegalStateException(
                    "jv-guard: cannot register '" + clazz.getSimpleName()
                            + "' - registry is already sealed"
            );
        }

        int found = 0;

        for (Method method : clazz.getDeclaredMethods()) {
            Task annotation = method.getAnnotation(Task.class);
            if (annotation == null) continue;

            // Make the method accessible once at registration time.
            // trySetAccessible() result gates whether we attempt handle construction --
            // if it returns false the method is inaccessible and a handle would also fail.
            boolean accessible = method.trySetAccessible();
            if (!accessible) {
                log.warn(
                        "could not set accessible on '" + method.getName()
                                + "' - invocation may fail at runtime",
                        clazz.getSimpleName()
                );
            }

            // Pre-build MethodHandle if CACHE = true and method is accessible.
            // For parameterized methods, look for a same-named static Object[] field
            // on the class first. If found, args are pre-bound into the handle at
            // seal time -- invokeExact() stays no-arg with constants baked in.
            // Handle is null if CACHE = false, inaccessible, or construction fails.
            MethodHandle handle = null;
            if (annotation.CACHE() && accessible) {
                Object[] boundArgs = (method.getParameterCount() > 0)
                        ? lookupArgsConstant(clazz, method.getName())
                        : null;
                handle = buildHandle(method, target, boundArgs, clazz.getSimpleName());
            }

            // Build rawHandle for MIRROR producers -- preserves return type for result capture.
            // Only built when a MIRROR target is declared and the method is accessible.
            String       mirror    = annotation.MIRROR();
            MethodHandle rawHandle = (!mirror.isEmpty() && accessible)
                    ? buildRawHandle(method, target, clazz.getSimpleName())
                    : null;

            pending.add(new TaskDescriptor(
                    clazz.getSimpleName(),
                    method.getName(),
                    annotation.POOL(),
                    annotation.EXECUTER(),
                    annotation.OP(),
                    annotation.GROUP(),
                    annotation.ORDER(),
                    annotation.LEAD(),
                    annotation.STATE_LOCK(),  // stateLock -- bypasses GROUP phase when false
                    mirror,
                    method,
                    target,
                    handle,
                    rawHandle,
                    null,   // mirrorOutputType -- resolved at seal() during chain walk
                    null    // mirrorInputType  -- resolved at seal() during chain walk
            ));
            found++;
        }

        if (found == 0) {
            log.warn("no @Task methods found in " + clazz.getSimpleName());
        } else {
            log.info("scanned " + clazz.getSimpleName()
                    + " - " + found + " task(s) queued");
        }
    }

    /**
     * Builds a pre-bound, no-arg MethodHandle for the given method.
     * <p>
     * For no-arg methods: binds the target instance and normalises the return
     * type to void. invokeExact() takes no arguments.
     * <p>
     * For parameterized methods: binds the target instance, then uses
     * MethodHandles.insertArguments() to pre-bind the constant args from the
     * same-named Object[] field. The result is still a no-arg void handle.
     * insertArguments() performs implicit unboxing (Integer -> int, etc.) so
     * boxed primitives in the Object[] match primitive parameter types cleanly.
     * A type mismatch here throws IllegalArgumentException, which the catch
     * converts to a null handle -- seal() blocks the task as Tier 1.
     * <p>
     * Returns null and logs a warning if construction fails for any reason.
     */
    private static MethodHandle buildHandle(
            Method method, Object target, Object[] boundArgs, String className
    ) {
        try {
            MethodHandle handle = MethodHandles.lookup().unreflect(method);
            if (target != null) {
                handle = handle.bindTo(target);
            }
            if (boundArgs != null && boundArgs.length > 0) {
                // Pre-bind constant args starting at position 0 (after instance bind).
                // This folds the arg values into the handle -- invokeExact() stays no-arg.
                handle = MethodHandles.insertArguments(handle, 0, boundArgs);
            }
            // Parameters remain unbound -- MIRROR consumers arrive here because their
            // argument comes from MirrorBus at runtime, not a pre-bound constant.
            // Return null so the executor takes the reflection path via invokeConsumer.
            if (handle.type().parameterCount() > 0) {
                return null;
            }

            // Normalise to a no-arg void handle regardless of declared return type.
            handle = handle.asType(MethodType.methodType(void.class));
            return handle;
        } catch (IllegalAccessException | IllegalArgumentException | WrongMethodTypeException e) {
            // IllegalAccessException  -- method not accessible despite trySetAccessible()
            // IllegalArgumentException -- type mismatch in insertArguments or asType;
            //                            seal() will report this as a Tier 1 violation
            log.warn(
                    "MethodHandle construction failed for '" + method.getName()
                            + "' - falling back to reflection",
                    className
            );
            return null;
        }
    }

    private static MethodHandle buildRawHandle(Method method, Object target, String className) {
        try {
            MethodHandle handle = MethodHandles.lookup().unreflect(method);
            if (target != null) handle = handle.bindTo(target);
            // No asType normalization -- return type preserved for result capture
            return handle;
        } catch (IllegalAccessException e) {
            log.warn("rawHandle construction failed for '" + method.getName()
                    + "' - MIRROR chain disabled for this producer", className);
            return null;
        }
    }

    /**
     * Looks up a same-named static Object[] field on the class to use as
     * pre-bound constant args for a parameterized @Task method.
     * <p>
     * Convention: declare a static final Object[] with the same name as the
     * decorated method in the same class:
     * <p>
     *     static final Object[] buildMultiJson = { "json", 100 };
     * <p>
     *     Task(POOL = "I", EXECUTER = "CEXC", OP = "buildMultiJson", GROUP = 0)
     *     public void buildMultiJson(String format, int limit) { ... }
     * <p>
     * The field must be static. Instance fields are ignored with a warning since
     * they carry per-instance state rather than constants, which breaks the
     * pre-binding model -- args are locked in at seal time, not per invocation.
     * <p>
     * Returns null (without error) when no matching field exists -- the absence
     * is only an error if the method is parameterized, which seal() checks.
     */
    private static Object[] lookupArgsConstant(Class<?> clazz, String methodName) {
        try {
            Field field = clazz.getDeclaredField(methodName);

            if (!Modifier.isStatic(field.getModifiers())) {
                log.warn(
                        "args constant '" + methodName + "' must be static - ignoring; "
                                + "instance fields carry per-object state, not constants",
                        clazz.getSimpleName()
                );
                return null;
            }

            field.trySetAccessible();
            Object value = field.get(null); // null receiver = static field

            if (value instanceof Object[] arr) {
                return arr;
            }

            log.warn(
                    "args constant '" + methodName + "' must be Object[] - ignoring",
                    clazz.getSimpleName()
            );
            return null;

        } catch (NoSuchFieldException e) {
            return null; // no constant defined -- not an error at this stage
        } catch (IllegalAccessException e) {
            log.warn(
                    "could not read args constant '" + methodName + "'",
                    clazz.getSimpleName()
            );
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Seal
    // -------------------------------------------------------------------------

    /**
     * Validates all pending tasks and locks the registry.
     * <p>
     * Tier 1 - POOL, EXECUTER, OP, GROUP, LEAD
     *   Hard block. All violations collected and thrown together in one error.
     * <p>
     * Tier 2 - ORDER (conditional on LEAD)
     *   LEAD >= 0 + invalid ORDER  -> hard block (promotion arithmetic breaks).
     *   LEAD == -1 + invalid ORDER -> auto-corrected to 0.0, warning logged.
     * <p>
     * Returns the sealed, unmodifiable task list.
     */
    public static List<TaskDescriptor> seal() {
        if (sealed.getAndSet(true)) {
            throw new IllegalStateException(
                    "jv-guard: seal() already called - registry cannot be re-sealed"
            );
        }

        List<String>         violations    = new ArrayList<>();
        List<TaskDescriptor> finalRegistry = new ArrayList<>();
        int                  cachedCount   = 0;

        // Pre-pass: collect MIRROR consumer OPs so they can be exempted from the
        // parameter check. Consumers receive their argument from MirrorBus at runtime,
        // not from a pre-bound args constant. Their parameter count is intentional.
        Set<String> mirrorConsumerOps = new HashSet<>();
        for (TaskDescriptor t : pending) {
            if (!t.mirror().isEmpty()) mirrorConsumerOps.add(t.mirror());
        }

        // Pre-pass: detect duplicate OP names before per-task validation.
        // Reports every method sharing a given OP so the user sees all conflicts
        // at once rather than discovering them one registration at a time.
        Map<String, List<String>> opToContexts = new LinkedHashMap<>();
        for (TaskDescriptor t : pending) {
            opToContexts.computeIfAbsent(t.op(), k -> new ArrayList<>()).add(t.context());
        }
        for (Map.Entry<String, List<String>> entry : opToContexts.entrySet()) {
            if (entry.getValue().size() > 1) {
                violations.add("[OP: '" + entry.getKey() + "'] registered by "
                        + entry.getValue().size() + " methods: "
                        + String.join(", ", entry.getValue())
                        + " - OP must be unique across all registered tasks");
            }
        }

        for (TaskDescriptor task : pending) {
            String  ctx        = task.context();
            boolean blocked    = false;
            double  finalOrder = task.order();

            // -----------------------------------------------------------------
            // Tier 1 - collect all hard violations before throwing
            // -----------------------------------------------------------------

            if (!VALID_POOLS.contains(task.pool())) {
                violations.add("[" + ctx + "] POOL: '" + task.pool()
                        + "' - must be I, P, H, or R");
                blocked = true;
            }

            if (!VALID_EXECUTERS.contains(task.executer())) {
                violations.add("[" + ctx + "] EXECUTER: '" + task.executer()
                        + "' - must be CEXC, IEXC, or AEXC");
                blocked = true;
            }

            if (task.op().isBlank()) {
                violations.add("[" + ctx + "] OP: cannot be empty or blank");
                blocked = true;
            }

            if (task.group() < 0) {
                violations.add("[" + ctx + "] GROUP: " + task.group()
                        + " - must be >= 0");
                blocked = true;
            }

            if (task.lead() < -1) {
                violations.add("[" + ctx + "] LEAD: " + task.lead()
                        + " - must be -1 (no affinity) or >= 0 (pinnacle rank)");
                blocked = true;
            }

            if (task.method().getParameterCount() > 0 && task.cachedHandle() == null
                    && !mirrorConsumerOps.contains(task.op())) {
                // Parameterized with no usable handle -- either no args constant was found,
                // the constant had a type mismatch, or CACHE=false was set explicitly.
                // All three cases leave us with no way to invoke the method correctly.
                violations.add("[" + ctx + "] '" + task.methodName() + "' has "
                        + task.method().getParameterCount() + " parameter(s) but no valid "
                        + "args constant was found or it failed type checking. Declare: "
                        + "static final Object[] " + task.methodName() + " = { arg1, arg2, ... }"
                        + " in " + task.className());
                blocked = true;
            }

            // -----------------------------------------------------------------
            // Tier 2 - ORDER, behaviour depends on LEAD
            // -----------------------------------------------------------------

            boolean orderInvalid = task.order() < 0.0 || task.order() >= 1.0;

            if (orderInvalid) {
                if (task.lead() >= 0) {
                    violations.add("[" + ctx + "] ORDER: " + task.order()
                            + " - must be in [0.0, 1.0) on LEAD tasks"
                            + " (suffix promotion requires a clean fractional part)");
                    blocked = true;
                } else {
                    log.warn("ORDER: " + task.order()
                            + " - invalid, defaulted to 0.0", ctx);
                    finalOrder = 0.0;
                }
            }

            if (!blocked) {
                TaskDescriptor final_ = task.withOrder(finalOrder);
                finalRegistry.add(final_);
                if (final_.cachedHandle() != null) cachedCount++;
            }
        }

        // Throw Tier 1/2 violations before chain walk -- chain walk uses opIndex
        if (!violations.isEmpty()) {
            throw new IllegalStateException(
                    "\njv-guard registration failed - fix before running:\n\n"
                            + String.join("\n", violations)
                            + "\n"
            );
        }

        sealedRegistry = List.copyOf(finalRegistry);
        opIndex = Map.copyOf(finalRegistry.stream()
                .collect(Collectors.toMap(TaskDescriptor::op, t -> t)));

        // Mirror chain validation pass -- runs after opIndex is built so lookups are O(1).
        // Validates type compatibility at each edge, writes types into descriptors.
        // Violations collected here throw separately after chain walk completes.
        Set<String>                  chainVisited    = new HashSet<>();
        Map<String, TaskDescriptor>  typedOverrides  = new HashMap<>();

        for (TaskDescriptor root : finalRegistry) {
            if (!root.isMirrorProducer()) continue;
            if (chainVisited.contains(root.op())) continue;

            TaskDescriptor current  = root;
            List<String>   chainOps = new ArrayList<>();

            while (current.isMirrorProducer()) {
                chainOps.add(current.op());
                chainVisited.add(current.op());

                String         targetOp = current.mirror();
                TaskDescriptor next     = opIndex.get(targetOp);

                if (next == null) {
                    violations.add("[MIRROR chain: " + String.join(" -> ", chainOps) + "]"
                            + " consumer OP '" + targetOp + "' not found in registry");
                    break;
                }

                Class<?> producerReturn = current.method().getReturnType();
                if (producerReturn == void.class) {
                    violations.add("[MIRROR] " + current.context()
                            + " returns void - cannot be a producer");
                    break;
                }

                Class<?>[] consumerParams = next.method().getParameterTypes();
                if (consumerParams.length == 0) {
                    violations.add("[MIRROR] consumer " + next.context()
                            + " must accept at least one parameter to receive the mirrored result");
                    break;
                }

                Class<?> consumerInput = consumerParams[0];
                if (!consumerInput.isAssignableFrom(producerReturn)) {
                    violations.add("[MIRROR chain: " + String.join(" -> ", chainOps)
                            + " -> " + next.op() + "]"
                            + " type mismatch: " + current.context()
                            + " returns " + producerReturn.getSimpleName()
                            + " but " + next.context()
                            + " expects " + consumerInput.getSimpleName());
                    break;
                }

                if ("CEXC".equals(next.executer())) {
                    violations.add("[MIRROR] consumer " + next.context()
                            + " must be AEXC or IEXC - CEXC blocks an OS thread awaiting result");
                    break;
                }

                // Edge is valid -- write types into both sides
                typedOverrides.put(current.op(),
                        current.withMirrorTypes(producerReturn, current.mirrorInputType()));
                typedOverrides.merge(next.op(),
                        next.withMirrorTypes(null, consumerInput),
                        (existing, incoming) -> existing.withMirrorTypes(
                                existing.mirrorOutputType(), incoming.mirrorInputType()));

                current = typedOverrides.getOrDefault(next.op(), next);
            }
        }

        // Throw chain violations
        if (!violations.isEmpty()) {
            throw new IllegalStateException(
                    "\njv-guard mirror chain validation failed:\n\n"
                            + String.join("\n", violations)
                            + "\n"
            );
        }

        // Rebuild registry and index with typed descriptors applied
        if (!typedOverrides.isEmpty()) {
            sealedRegistry = finalRegistry.stream()
                    .map(t -> typedOverrides.getOrDefault(t.op(), t))
                    .toList();

            opIndex = Map.copyOf(sealedRegistry.stream()
                    .collect(Collectors.toMap(TaskDescriptor::op, t -> t)));
        }

        log.info("registry sealed - "
                + sealedRegistry.size() + " task(s) ready  "
                + "(" + cachedCount + " cached, "
                + (sealedRegistry.size() - cachedCount) + " reflection)");

        return sealedRegistry;
    }

    /**
     * Returns the sealed registry. Only valid after seal() succeeds.
     */
    public static List<TaskDescriptor> getSealedRegistry() {
        if (sealedRegistry == null) {
            throw new IllegalStateException(
                    "jv-guard: registry not yet sealed - call seal() first"
            );
        }
        return sealedRegistry;
    }

    /**
     * Looks up a TaskDescriptor by its OP label. O(1) via the sealed index.
     * Returns null if no task with that OP is registered.
     */
    public static TaskDescriptor getByOp(String op) {
        if (opIndex == null) {
            throw new IllegalStateException(
                    "jv-guard: registry not yet sealed - call seal() first"
            );
        }
        return opIndex.get(op);
    }

    // -------------------------------------------------------------------------
    // TaskDescriptor
    // -------------------------------------------------------------------------

    /**
     * Immutable snapshot of a single @Task decorated method.
     * <p>
     * Built at registration time from annotation values and the target instance.
     * ORDER may be corrected at seal() via withOrder(). cachedHandle is built
     * during registration for CACHE=true methods and carried through unchanged.
     * <p>
     * cachedHandle: pre-bound no-arg MethodHandle. null if CACHE=false,
     *               if trySetAccessible() failed, or if handle construction failed.
     *               Executers call invokeExact() when non-null, reflection otherwise.
     * <p>
     * target: the object instance to invoke the method on.
     *         null for static @Task methods - Method.invoke(null) is correct.
     */
    public record TaskDescriptor(
            String       className,
            String       methodName,
            String       pool,
            String       executer,
            String       op,
            int          group,
            double       order,
            int          lead,
            boolean      stateLock,          // STATE_LOCK annotation value
            String       mirror,
            Method       method,
            Object       target,
            MethodHandle cachedHandle,
            MethodHandle rawHandle,         // return-preserving handle, null if not producer
            Class<?>     mirrorOutputType,  // producer's declared return type
            Class<?>     mirrorInputType    // consumer's declared first param type
    ) {
        /** Canonical context string embedded in all log and error output. */
        public String  context()          { return className + "." + methodName; }
        public boolean isMirrorProducer() { return mirror != null && !mirror.isEmpty(); }
        public boolean isMirrorConsumer() { return mirrorInputType != null; }

        /**
         * True when this task bypasses GROUP phase and skip-list ordering.
         * Stateless leads dispatch directly to any free worker via work signal.
         * Condition: STATE_LOCK = false AND LEAD >= 0.
         */
        public boolean isStatelessLead()  { return !stateLock && lead >= 0; }

        public TaskDescriptor withOrder(double correctedOrder) {
            return new TaskDescriptor(
                    className, methodName, pool, executer, op,
                    group, correctedOrder, lead, stateLock, mirror,
                    method, target, cachedHandle, rawHandle,
                    mirrorOutputType, mirrorInputType
            );
        }

        /** Called during seal() chain walk to write resolved types back into the descriptor. */
        public TaskDescriptor withMirrorTypes(Class<?> outputType, Class<?> inputType) {
            return new TaskDescriptor(
                    className, methodName, pool, executer, op,
                    group, order, lead, stateLock, mirror,
                    method, target, cachedHandle, rawHandle,
                    outputType, inputType
            );
        }
    }
}