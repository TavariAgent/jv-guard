package jvguard;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * jv-guard -- extended smoke test + performance batch test
 *
 * Smoke test task graph (9 total, all call done()):
 *
 *   main submits:
 *     asyncWork  (P pool orchestrator)
 *     ioWork     (I pool, IEXC)
 *     paramWork  (I pool, CEXC, pre-bound args)
 *
 *   asyncWork triggers:
 *     leadWork   (I pool, LEAD:0, heavy hash)
 *     cpuWork    (H pool, LEAD:1, randomized harmonic hash)
 *     asyncHelper (P pool, ORDER:0.7, post-orchestration)
 *
 *   leadWork triggers:
 *     leadHelper  (I pool, ORDER:0.5, lighter follow-up hash)
 *
 *   cpuWork triggers:
 *     cpuHelper   (H pool, ORDER:0.6, pre-bound rounds)
 *
 *   ioWork triggers:
 *     ioHelper    (I pool, IEXC, ORDER:0.6, aggregation)
 */
public class Main {

    // =========================================================================
    // Task service
    // =========================================================================

    static class TaskService {

        // Swappable latch -- smoke test sets it at construction,
        // batch test swaps it between runs via setLatch().
        private volatile CountDownLatch activeLatch;

        private static final Random RNG = new Random();

        TaskService(CountDownLatch initial) {
            this.activeLatch = initial;
        }

        void setLatch(CountDownLatch latch) {
            this.activeLatch = latch;
        }

        private void done() {
            CountDownLatch l = activeLatch;
            if (l != null) l.countDown();
        }

        // ---------------------------------------------------------------------
        // Primary tasks
        // ---------------------------------------------------------------------

        /**
         * leadWork -- I pool, LEAD:0
         * Highest-priority task. Always pulled from the back segment before
         * any front-queue task. Heavy 200-round SHA-256 chain.
         * Spawns leadHelper when complete.
         */
        @Task(POOL = "I", EXECUTER = "CEXC", OP = "leadWork",
                GROUP = 0, ORDER = 0.001, LEAD = 0)
        public void leadWork() {
            byte[] result = hashChain("lead-priority-payload", 200);
            System.out.printf(
                    "  [leadWork]    LEAD:0  rounds=200  hash=%s  thread: %s%n",
                    hexPrefix(result), Thread.currentThread().getName());
            Handler.get().submit("leadHelper");
            done();
        }

        /**
         * cpuWork -- H pool (harmonic), LEAD:1
         * Randomized 50-200 round hash chain. Variable execution time is
         * intentional -- it's what harmonic work means: static difficulty
         * class, non-uniform actual cost. Spawns cpuHelper when complete.
         */
        @Task(POOL = "H", EXECUTER = "CEXC", OP = "cpuWork",
                GROUP = 0, ORDER = 0.001, LEAD = 1)
        public void cpuWork() {
            int rounds = 50 + RNG.nextInt(150);
            byte[] result = hashChain("harmonic-cpu-payload", rounds);
            System.out.printf(
                    "  [cpuWork]     LEAD:1  rounds=%-3d  hash=%s  thread: %s%n",
                    rounds, hexPrefix(result), Thread.currentThread().getName());
            Handler.get().submit("cpuHelper");
            done();
        }

        /**
         * ioWork -- I pool, IEXC (virtual thread)
         * Builds a 1000-record payload then parks for 2ms to simulate IO
         * latency. Virtual thread parks here -- the carrier is freed and
         * picks up the next virtual thread immediately. Spawns ioHelper.
         */
        @Task(POOL = "I", EXECUTER = "IEXC", OP = "ioWork",
                GROUP = 0, ORDER = 0.002)
        public void ioWork() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append("io-record-").append(i).append(";");
            }
            int bytes = sb.length();
            try { Thread.sleep(2); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.printf(
                    "  [ioWork]      IEXC    bytes=%-6d  thread: %s%n",
                    bytes, Thread.currentThread().getName());
            Handler.get().submit("ioHelper");
            done();
        }

        /**
         * asyncWork -- P pool, GROUP 0, no LEAD (orchestrator protocol)
         * Runs early in the P pool front queue. Triggers LEAD tasks in
         * I pool and H pool -- cross-pool coordination from a single
         * protocol submission point. No CPU work; pure orchestration.
         * Spawns asyncHelper as its own post-orchestration follow-up.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "asyncWork",
                GROUP = 0, ORDER = 0.001)
        public void asyncWork() {
            System.out.printf(
                    "  [asyncWork]   P-orch  -> leadWork + cpuWork  thread: %s%n",
                    Thread.currentThread().getName());
            // Cross-pool triggers: P pool drives I-pool LEAD:0 and H-pool LEAD:1
            Handler.get().submit("leadWork");
            Handler.get().submit("cpuWork");
            Handler.get().submit("asyncHelper");
            done();
        }

        /**
         * paramWork -- I pool, CEXC, pre-bound constant args
         * Constants declared below are folded into the MethodHandle at seal()
         * time. invokeExact() takes no args -- same hot path as no-arg tasks.
         */
        static final Object[] paramWork = { "batch-record", 42 };

        @Task(POOL = "I", EXECUTER = "CEXC", OP = "paramWork",
                GROUP = 0, ORDER = 0.003)
        public void paramWork(String label, int count) {
            byte[] result = hashChain(label + "-" + count, count);
            System.out.printf(
                    "  [paramWork]   CEXC    label=%s count=%d  hash=%s  thread: %s%n",
                    label, count, hexPrefix(result), Thread.currentThread().getName());
            done();
        }

        // ---------------------------------------------------------------------
        // Helper tasks -- double ORDER values, spawned from within primaries
        // Each runs on its own thread via the executor after being submitted
        // by its primary. ORDER >= 0.5 keeps them behind the primary tasks.
        // ---------------------------------------------------------------------

        /**
         * leadHelper -- I pool, CEXC, ORDER:0.5
         * Lighter 50-round follow-up hash. Runs after the heavy leadWork
         * chain, using the same warm cache lines on the I pool core.
         */
        @Task(POOL = "I", EXECUTER = "CEXC", OP = "leadHelper",
                GROUP = 0, ORDER = 0.5)
        public void leadHelper() {
            byte[] result = hashChain("lead-follow-up", 50);
            System.out.printf(
                    "  [leadHelper]    ORDER:0.5  hash=%s  thread: %s%n",
                    hexPrefix(result), Thread.currentThread().getName());
            done();
        }

        /**
         * cpuHelper -- H pool, CEXC, ORDER:0.6, pre-bound rounds
         * Assist task for cpuWork on the same H pool core group.
         * Fixed 75 rounds -- lighter and deterministic unlike its primary.
         */
        static final Object[] cpuHelper = { 75 };

        @Task(POOL = "H", EXECUTER = "CEXC", OP = "cpuHelper",
                GROUP = 0, ORDER = 0.6)
        public void cpuHelper(int rounds) {
            byte[] result = hashChain("cpu-assist", rounds);
            System.out.printf(
                    "  [cpuHelper]     ORDER:0.6  rounds=%d  hash=%s  thread: %s%n",
                    rounds, hexPrefix(result), Thread.currentThread().getName());
            done();
        }

        /**
         * ioHelper -- I pool, IEXC, ORDER:0.6
         * Aggregates a checksum as post-IO validation. Virtual thread,
         * no blocking -- fast lightweight follow-up to ioWork's payload build.
         */
        @Task(POOL = "I", EXECUTER = "IEXC", OP = "ioHelper",
                GROUP = 0, ORDER = 0.6)
        public void ioHelper() {
            long checksum = 0;
            for (int i = 0; i < 1000; i++) checksum += i * 31L;
            System.out.printf(
                    "  [ioHelper]      ORDER:0.6  checksum=%d  thread: %s%n",
                    checksum, Thread.currentThread().getName());
            done();
        }

        /**
         * asyncHelper -- P pool, AEXC, ORDER:0.7
         * Post-orchestration follow-up on the P pool. Runs after asyncWork
         * has finished firing its cross-pool triggers. Confirms the P pool
         * queue drains its helpers in ORDER sequence.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "asyncHelper",
                GROUP = 0, ORDER = 0.7)
        public void asyncHelper() {
            System.out.printf(
                    "  [asyncHelper]   ORDER:0.7  protocol complete  thread: %s%n",
                    Thread.currentThread().getName());
            done();
        }

        // ---------------------------------------------------------------------
        // Batch task -- performance test only
        // Light SHA-256 work to isolate engine scheduling overhead.
        // Not submitted in the smoke test.
        // ---------------------------------------------------------------------

        @Task(POOL = "I", EXECUTER = "CEXC", OP = "batchWork",
                GROUP = 0, ORDER = 0.001)
        public void batchWork() {
            hashChain("batch", 25);
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

        System.out.println("=== jv-guard extended test ===\n");

        // Smoke test latch -- 9 tasks, each calls done() exactly once:
        // asyncWork + ioWork + paramWork          (3 from main)
        // leadWork + cpuWork + asyncHelper        (3 from asyncWork)
        // leadHelper                              (1 from leadWork)
        // cpuHelper                               (1 from cpuWork)
        // ioHelper                                (1 from ioWork)
        CountDownLatch smokeLatch = new CountDownLatch(9);
        TaskService    service    = new TaskService(smokeLatch);

        // --- Setup ---
        System.out.println(">> registering...");
        Wrapping.register(service);

        System.out.println(">> sealing...");
        Wrapping.seal();

        // Pool config:
        //   I pool (2 cores): leadWork, ioWork, paramWork, leadHelper, ioHelper, batchWork
        //   P pool (1 core):  asyncWork, asyncHelper
        //   H pool (2 cores): cpuWork, cpuHelper
        Pools.PoolConfig config = new Pools.PoolConfig.Builder()
                .iPool(4)
                .pPool(2)
                .hPool(2)
                .rPool(0)
                .workersPerCore(1)
                .build();

        System.out.println(">> initializing handler...");
        Handler.initialize(config);
        MirrorBus.initialize();
        System.out.println(">> starting executors...\n");
        Executers.start();

        // --- Smoke test ---
        System.out.println(">> submitting smoke test tasks...\n");

        // asyncWork on P pool orchestrates leadWork (I, LEAD:0) + cpuWork (H, LEAD:1)
        // ioWork and paramWork go directly to I pool front queue
        Handler.get().submit("asyncWork");
        Handler.get().submit("ioWork");
        Handler.get().submit("paramWork");

        boolean smokeOk = smokeLatch.await(10, TimeUnit.SECONDS);
        System.out.println();
        if (smokeOk) {
            System.out.println(">> smoke test passed -- all 9 tasks completed");
        } else {
            System.out.printf(">> TIMEOUT -- %d task(s) did not complete%n",
                    smokeLatch.getCount());
        }

        // --- Performance batch test ---
        System.out.println("\n=== Performance Batch Test ===");
        System.out.println("  task: batchWork (SHA-256 x25, I pool, CEXC)\n");

        int[] batchSizes = { 1_000, 5_000, 10_000 , 100_000, 1_000_000, 10_000_000 };

        for (int size : batchSizes) {
            CountDownLatch batchLatch = new CountDownLatch(size);
            service.setLatch(batchLatch);

            long start = System.nanoTime();

            for (int i = 0; i < size; i++) {
                Handler.get().submit("batchWork");
                if (i % 10_000 == 0) Thread.yield();
            }

            boolean batchOk = batchLatch.await(240, TimeUnit.SECONDS);
            long    elapsed  = System.nanoTime() - start;

            if (batchOk) {
                double seconds    = elapsed / 1_000_000_000.0;
                long   throughput = (long)(size / seconds);
                System.out.printf("  n=%-6d  time=%.3fs  throughput=%,d tasks/s%n",
                        size, seconds, throughput);
            } else {
                System.out.printf("  n=%-6d  TIMEOUT -- %d remaining%n",
                        size, batchLatch.getCount());
            }
        }

        // --- Shutdown ---
        System.out.println("\n>> shutting down...");
        Executers.shutdown();
        Pools.getRegistry().shutdown();
        System.out.println("\n=== done ===");
    }
}