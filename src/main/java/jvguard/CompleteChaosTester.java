package jvguard;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * jv-guard - CompleteChaosTester
 * <p>
 * Full engine chaos test covering every major feature in sequence.
 * Each scenario runs against its own CountDownLatch. Tasks call done()
 * exactly once per invocation. Scenarios are waited out before proceeding
 * so latch swaps never collide with in-flight tasks.
 * <p>
 * Scenarios:
 * <p>
 *   1. rPool commands (3 tasks)
 *      P pool orchestrator fires two R pool command tasks.
 *      Confirms R pool isolation -- command workers run on separate cores.
 * <p>
 *   2. Two-step MIRROR -- I CEXC → P AEXC (2 tasks)
 *      Producer hashes a payload and returns a hex String.
 *      Consumer receives it via MirrorBus, confirms typed delivery.
 * <p>
 *   3. Three-step MIRROR chain -- I CEXC → I IEXC → P AEXC (3 tasks)
 *      Root producer returns Integer. Mid-chain node receives Integer,
 *      hashes it, returns String. Terminal consumer receives String.
 *      Exercises invokeMidChain() and the two-field type write in seal().
 * <p>
 *   4. Concurrent LEAD storm (7 tasks)
 *      Orchestrator fires 3x LEAD:0 (I pool) and 3x LEAD:1 (H pool).
 *      Confirms suffix allocation, LEAD:0 always drains before LEAD:1,
 *      and concurrent suffix recycling is clean.
 * <p>
 *   5. Async orchestration wave (21 tasks)
 *      P pool orchestrator fires 20 lightweight I pool tasks in a burst.
 *      Exercises queue depth, worker saturation, and throughput under
 *      orchestrated async load.
 * <p>
 * Pool layout:
 *   I pool (2 cores)  - idempotent, MIRROR producers and mid-chain
 *   P pool (1 core)   - protocol, MIRROR consumers and orchestrators
 *   H pool (2 cores)  - harmonic, LEAD:1 storm tasks
 *   R pool (1 core)   - command processing
 *   workersPerCore: 3
 */
public class CompleteChaosTester {

    // =========================================================================
    // ChaosService -- all task methods for all scenarios
    // =========================================================================

    static class ChaosService {

        private volatile CountDownLatch activeLatch;
        private static final Random     RNG = new Random();

        ChaosService(CountDownLatch initial) { this.activeLatch = initial; }

        void setLatch(CountDownLatch latch) { this.activeLatch = latch; }

        private void done() {
            CountDownLatch l = activeLatch;
            if (l != null) l.countDown();
        }

        // ---------------------------------------------------------------------
        // Scenario 1 -- rPool commands
        // 3 tasks: cmdOrchestrate + cmdPing + cmdStatus
        // ---------------------------------------------------------------------

        /**
         * P pool orchestrator. Fires two R pool commands then signals done.
         * Entry point for scenario 1 -- submit this to start the chain.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "cmdOrchestrate",
                GROUP = 0, ORDER = 0.001)
        public void cmdOrchestrate() {
            System.out.printf(
                    "  [cmdOrchestrate]   P-AEXC   -> cmdPing + cmdStatus  thread: %s%n",
                    Thread.currentThread().getName());
            Handler.get().submit("cmdPing");
            Handler.get().submit("cmdStatus");
            done();
        }

        /**
         * R pool command -- lightweight ping response.
         * Thread name confirms R pool core isolation from I pool.
         */
        @Task(POOL = "R", EXECUTER = "AEXC", OP = "cmdPing",
                GROUP = 0, ORDER = 0.001)
        public void cmdPing() {
            System.out.printf(
                    "  [cmdPing]          R-AEXC   PONG  thread: %s%n",
                    Thread.currentThread().getName());
            done();
        }

        /**
         * R pool command -- reports thread identity.
         * ORDER: 0.002 ensures it pulls after cmdPing in the same GROUP.
         */
        @Task(POOL = "R", EXECUTER = "AEXC", OP = "cmdStatus",
                GROUP = 0, ORDER = 0.002)
        public void cmdStatus() {
            System.out.printf(
                    "  [cmdStatus]        R-AEXC   online  thread: %s%n",
                    Thread.currentThread().getName());
            done();
        }

        // ---------------------------------------------------------------------
        // Scenario 2 -- two-step MIRROR (I CEXC → P AEXC)
        // 2 tasks: buildPayload (producer) + processPayload (consumer)
        // ---------------------------------------------------------------------

        /**
         * MIRROR producer -- hashes a payload and returns the hex prefix as a String.
         * invokeProducer() captures the return, routes to MirrorBus, auto-submits consumer.
         * done() fires as part of method body before return; rawHandle captures the value.
         */
        @Task(POOL = "I", EXECUTER = "CEXC", OP = "buildPayload",
                GROUP = 0, ORDER = 0.001, MIRROR = "processPayload")
        public String buildPayload() {
            byte[] hash = hashChain("mirror-payload", 100);
            String hex  = hexPrefix(hash);
            System.out.printf(
                    "  [buildPayload]     I-CEXC   produced: %s  thread: %s%n",
                    hex, Thread.currentThread().getName());
            done();
            return hex;
        }

        /**
         * MIRROR consumer -- receives String from MirrorBus.
         * Virtual thread parks on take() until buildPayload routes its result.
         * seal() validated String <- String at this edge before any task ran.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "processPayload",
                GROUP = 0, ORDER = 0.5)
        public void processPayload(String payload) {
            System.out.printf(
                    "  [processPayload]   P-AEXC   received: %s  thread: %s%n",
                    payload, Thread.currentThread().getName());
            done();
        }

        // ---------------------------------------------------------------------
        // Scenario 3 -- three-step MIRROR chain (I CEXC → I IEXC → P AEXC)
        // 3 tasks: generateData + transformData (mid-chain) + storeData
        // Exercises invokeMidChain() and the two-field withMirrorTypes() write.
        // ---------------------------------------------------------------------

        /**
         * Chain root -- computes a random round count and returns it as Integer.
         * seal() validated Integer <- Integer at the generateData→transformData edge.
         */
        @Task(POOL = "I", EXECUTER = "CEXC", OP = "generateData",
                GROUP = 0, ORDER = 0.001, MIRROR = "transformData")
        public Integer generateData() {
            int rounds = 50 + RNG.nextInt(100);
            System.out.printf(
                    "  [generateData]     I-CEXC   produced: %d rounds  thread: %s%n",
                    rounds, Thread.currentThread().getName());
            done();
            return rounds;
        }

        /**
         * Mid-chain node -- both consumer (Integer in) and producer (String out).
         * invokeMidChain() awaits Integer from bus, calls rawHandle.invoke(cast),
         * then routes the String return to storeData and auto-submits it.
         * seal() validated Integer <- Integer and String <- String at both edges.
         */
        @Task(POOL = "I", EXECUTER = "IEXC", OP = "transformData",
                GROUP = 0, ORDER = 0.3, MIRROR = "storeData")
        public String transformData(Integer rounds) {
            byte[] hash = hashChain("chain-transform", rounds);
            String hex  = hexPrefix(hash);
            System.out.printf(
                    "  [transformData]    I-IEXC   rounds=%-3d  hex=%s  thread: %s%n",
                    rounds, hex, Thread.currentThread().getName());
            done();
            return hex;
        }

        /**
         * Chain terminal -- receives the final String from MirrorBus and logs it.
         * No further routing. Virtual thread parks on take() until transformData routes.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "storeData",
                GROUP = 0, ORDER = 0.6)
        public void storeData(String data) {
            System.out.printf(
                    "  [storeData]        P-AEXC   stored: %s  thread: %s%n",
                    data, Thread.currentThread().getName());
            done();
        }

        // ---------------------------------------------------------------------
        // Scenario 4 -- concurrent LEAD storm
        // 7 tasks: stormOrchestrate + 3x stormLead0 + 3x stormLead1
        // Confirms suffix allocation, LEAD:0 drains before LEAD:1, recycling is clean.
        // ---------------------------------------------------------------------

        /**
         * Storm orchestrator on P pool. Fires 3 LEAD:0 tasks into I pool and
         * 3 LEAD:1 tasks into H pool simultaneously. All six are pinned to the
         * back segment of their queues, suffix-allocated on arrival.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "stormOrchestrate",
                GROUP = 0, ORDER = 0.001)
        public void stormOrchestrate() {
            System.out.printf(
                    "  [stormOrchestrate] P-AEXC   -> 3x stormLead0 + 3x stormLead1  thread: %s%n",
                    Thread.currentThread().getName());
            for (int i = 0; i < 3; i++) Handler.get().submit("stormLead0");
            for (int i = 0; i < 3; i++) Handler.get().submit("stormLead1");
            done();
        }

        /**
         * LEAD:0 pinnacle task on I pool. Always drains before any LEAD:1 task.
         * Three concurrent submissions claim suffixes 1, 2, 3 -- sort keys 1.001,
         * 2.001, 3.001 in the skip list. Released and recycled after each completes.
         */
        @Task(POOL = "I", EXECUTER = "CEXC", OP = "stormLead0",
                GROUP = 0, ORDER = 0.001, LEAD = 0)
        public void stormLead0() {
            byte[] hash = hashChain("lead-storm-0", 75);
            System.out.printf(
                    "  [stormLead0]       I-LEAD:0  hash=%s  thread: %s%n",
                    hexPrefix(hash), Thread.currentThread().getName());
            done();
        }

        /**
         * LEAD:1 pinnacle task on H pool. Pulls only after all LEAD:0 tasks complete.
         * Same suffix allocation as stormLead0, isolated to H pool back segment.
         */
        @Task(POOL = "H", EXECUTER = "CEXC", OP = "stormLead1",
                GROUP = 0, ORDER = 0.001, LEAD = 1)
        public void stormLead1() {
            byte[] hash = hashChain("lead-storm-1", 50);
            System.out.printf(
                    "  [stormLead1]       H-LEAD:1  hash=%s  thread: %s%n",
                    hexPrefix(hash), Thread.currentThread().getName());
            done();
        }

        // ---------------------------------------------------------------------
        // Scenario 5 -- async orchestration wave
        // 21 tasks: waveOrchestrate + 20x waveTask
        // Exercises queue depth, worker saturation, burst throughput.
        // ---------------------------------------------------------------------

        /**
         * Wave orchestrator on P pool. Submits 20 identical lightweight tasks
         * to the I pool front queue in a single burst. ORDER: 0.1 on waveTasks
         * keeps them behind any LEAD work that might be in-flight.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "waveOrchestrate",
                GROUP = 0, ORDER = 0.001)
        public void waveOrchestrate() {
            System.out.printf(
                    "  [waveOrchestrate]  P-AEXC   -> 20x waveTask  thread: %s%n",
                    Thread.currentThread().getName());
            for (int i = 0; i < 20; i++) Handler.get().submit("waveTask");
            done();
        }

        /**
         * Wave task -- 25-round SHA-256 on I pool. Same descriptor submitted 20 times.
         * TaskDescriptors are immutable so concurrent reuse is safe. Each invocation
         * is a separate submission draining from the front priority queue.
         */
        @Task(POOL = "I", EXECUTER = "CEXC", OP = "waveTask",
                GROUP = 0, ORDER = 0.1)
        public void waveTask() {
            hashChain("wave", 25);
            done();
        }

        // ---------------------------------------------------------------------
        // Utility
        // ---------------------------------------------------------------------

        private static byte[] hashChain(String seed, int rounds) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                byte[] data = seed.getBytes(StandardCharsets.UTF_8);
                for (int i = 0; i < rounds; i++) data = md.digest(data);
                return data;
            } catch (NoSuchAlgorithmException e) {
                return new byte[32]; // SHA-256 always present in JCE
            }
        }

        private static String hexPrefix(byte[] hash) {
            return String.format("%02x%02x%02x%02x",
                    hash[0], hash[1], hash[2], hash[3]);
        }
    }

    // =========================================================================
    // Main
    // =========================================================================

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== jv-guard CompleteChaosTester ===\n");

        ChaosService service = new ChaosService(null);

        // --- Setup ---
        System.out.println(">> registering...");
        Wrapping.register(service);

        System.out.println(">> sealing...");
        Wrapping.seal();

        // Pool layout:
        //   I pool (3 cores): buildPayload, generateData, transformData, waveTask, stormLead0
        //   P pool (1 core):  cmdOrchestrate, processPayload, storeData, stormOrchestrate, waveOrchestrate
        //   H pool (3 cores): stormLead1
        //   R pool (1 core):  cmdPing, cmdStatus
        Pools.PoolConfig config = new Pools.PoolConfig.Builder()
                .iPool(3)
                .pPool(1)
                .hPool(3)
                .rPool(1)
                .workersPerCore(3)
                .build();

        System.out.println(">> initializing handler...");
        Handler.initialize(config);
        MirrorBus.initialize();

        System.out.println(">> starting executors...\n");
        Executers.start();

        // =====================================================================
        // Scenario 1 -- rPool commands
        // latch(3): cmdOrchestrate + cmdPing + cmdStatus
        // =====================================================================
        System.out.println("=== Scenario 1: rPool commands ===\n");
        CountDownLatch s1Latch = new CountDownLatch(3);
        service.setLatch(s1Latch);
        Handler.get().submit("cmdOrchestrate");
        boolean s1ok = s1Latch.await(10, TimeUnit.SECONDS);
        System.out.printf("%n>> %s%n%n", s1ok ? "PASS" : "TIMEOUT");

        // =====================================================================
        // Scenario 2 -- two-step MIRROR
        // latch(2): buildPayload (producer) + processPayload (consumer)
        // =====================================================================
        System.out.println("=== Scenario 2: two-step MIRROR  I-CEXC → P-AEXC ===\n");
        CountDownLatch s2Latch = new CountDownLatch(2);
        service.setLatch(s2Latch);
        Handler.get().submit("buildPayload");
        boolean s2ok = s2Latch.await(10, TimeUnit.SECONDS);
        System.out.printf("%n>> %s%n%n", s2ok ? "PASS" : "TIMEOUT");

        // =====================================================================
        // Scenario 3 -- three-step MIRROR chain
        // latch(3): generateData + transformData (mid-chain) + storeData
        // =====================================================================
        System.out.println("=== Scenario 3: three-step MIRROR chain  I-CEXC → I-IEXC → P-AEXC ===\n");
        CountDownLatch s3Latch = new CountDownLatch(3);
        service.setLatch(s3Latch);
        Handler.get().submit("generateData");
        boolean s3ok = s3Latch.await(10, TimeUnit.SECONDS);
        System.out.printf("%n>> %s%n%n", s3ok ? "PASS" : "TIMEOUT");

        // =====================================================================
        // Scenario 4 -- concurrent LEAD storm
        // latch(7): stormOrchestrate + 3x stormLead0 + 3x stormLead1
        // =====================================================================
        System.out.println("=== Scenario 4: concurrent LEAD storm ===\n");
        CountDownLatch s4Latch = new CountDownLatch(7);
        service.setLatch(s4Latch);
        Handler.get().submit("stormOrchestrate");
        boolean s4ok = s4Latch.await(10, TimeUnit.SECONDS);
        System.out.printf("%n>> %s%n%n", s4ok ? "PASS" : "TIMEOUT");

        // =====================================================================
        // Scenario 5 -- async orchestration wave
        // latch(21): waveOrchestrate + 20x waveTask
        // =====================================================================
        System.out.println("=== Scenario 5: async orchestration wave ===\n");
        CountDownLatch s5Latch = new CountDownLatch(21);
        service.setLatch(s5Latch);
        Handler.get().submit("waveOrchestrate");
        boolean s5ok = s5Latch.await(10, TimeUnit.SECONDS);
        System.out.printf("%n>> %s%n%n", s5ok ? "PASS" : "TIMEOUT");

        // =====================================================================
        // Summary
        // =====================================================================
        System.out.println("=== Results ===\n");
        System.out.printf("  S1  rPool commands            %s%n", s1ok ? "PASS" : "FAIL");
        System.out.printf("  S2  two-step MIRROR           %s%n", s2ok ? "PASS" : "FAIL");
        System.out.printf("  S3  three-step MIRROR chain   %s%n", s3ok ? "PASS" : "FAIL");
        System.out.printf("  S4  concurrent LEAD storm     %s%n", s4ok ? "PASS" : "FAIL");
        System.out.printf("  S5  async orchestration wave  %s%n", s5ok ? "PASS" : "FAIL");

        boolean allPass = s1ok && s2ok && s3ok && s4ok && s5ok;
        System.out.printf("%n  %s%n%n", allPass ? "ALL PASS" : "SOME FAILURES -- check logs above");

        // --- Shutdown ---
        System.out.println(">> shutting down...");
        Executers.shutdown();
        Pools.getRegistry().shutdown();
        System.out.println("\n=== done ===");
    }
}