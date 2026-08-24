package jvguard;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * jv-guard - GroupSeries
 *
 * Demonstrates GROUP-ordered MIRROR chains by computing two mathematical
 * constants in sequence, with the first result feeding the second.
 *
 * ── Stage 1: Leibniz series for PI ──────────────────────────────────────────
 *
 *   π/4 = Σ(k=0..∞) (−1)^k / (2k+1)   →   π = 4 × (1 − 1/3 + 1/5 − ...)
 *
 *   10 GROUP stages × 10M terms = 100M total.
 *   Each stage is a separate @Task on the I pool computing one chunk.
 *   MIRROR carries the partial sum (Double) from stage to stage.
 *
 * ── Stage 2: Glaisher-Kinkelin constant A via Barnes G ──────────────────────
 *
 *   Uses the Barnes G asymptotic expansion:
 *     ln G(N+2) = Σ(j=1..N) (N−j+1)·ln(j)                  [sum S]
 *     ln(A)     = S − (N²/2 + N/2)·ln(N) + 3N²/4 − N/2·ln(2π) + 1/12·ln(N)
 *
 *   The ln(2π) correction is computed using the PI received from the Leibniz
 *   chain. Leibniz approximation error propagates into A -- this is intentional:
 *   it demonstrates typed error propagation across the MIRROR boundary.
 *
 *   10 GROUP stages × 1,000 terms (j) = N=10,000 total.
 *   Each stage accumulates a chunk of (N−j+1)·ln(j) on the P pool.
 *   Expected accuracy: ~6 significant digits of A (double-precision limit at N=10k).
 *
 * ── MIRROR type chain ────────────────────────────────────────────────────────
 *
 *   leibnizOp0  -> Double          -> leibnizOp1          (root producer, CEXC)
 *   leibnizOp1  -> Double          -> leibnizOp2          (mid-chain, IEXC)
 *   ...
 *   leibnizOp8  -> Double          -> leibnizOp9          (mid-chain, IEXC)
 *   leibnizOp9  -> GlaishierState  -> glashierOp0         (bridge: Double in, State out)
 *   glashierOp0 -> GlaishierState  -> glashierOp1         (mid-chain, AEXC)
 *   ...
 *   glashierOp8 -> GlaishierState  -> glashierOp9         (mid-chain, AEXC)
 *   glashierOp9 : terminal consumer, applies final formula
 *
 *   All 20 edges are type-validated at seal() before any task runs.
 *   A type mismatch at any edge is reported with the exact chain location.
 *
 * ── GROUP role ───────────────────────────────────────────────────────────────
 *
 *   In this design, MIRROR auto-submit enforces the execution order -- each
 *   stage is submitted only after the previous completes. GROUP labels each
 *   stage correctly and would be the primary ordering mechanism if tasks were
 *   pre-submitted simultaneously (batch mode) rather than chained.
 *   GROUP and MIRROR are complementary tools: GROUP for queue ordering,
 *   MIRROR for typed data-dependency sequencing.
 *
 * ── Task count ───────────────────────────────────────────────────────────────
 *
 *   chainStagger  1
 *   leibnizOp0-9  10  (GROUP 0-9 on I pool)
 *   glashierOp0-9 10  (GROUP 0-9 on P pool)
 *   Total:        21  all call done() exactly once
 *
 * ── Expected runtime ─────────────────────────────────────────────────────────
 *
 *   ~1-2 seconds (100M floating-point additions dominate; Glaisher is fast).
 */
public class GroupSeries {

    // =========================================================================
    // GlaishierState -- typed value crossing the Leibniz -> Glaisher boundary
    // =========================================================================

    /**
     * Immutable state container passed through the Glaisher MIRROR chain.
     * Each mid-chain node returns a new instance with sumSoFar and nextJ updated.
     *
     *   pi:       computed PI from the completed Leibniz chain
     *   sumSoFar: Σ(j=1..nextJ-1) (N−j+1)·ln(j) accumulated so far
     *   nextJ:    index of the next j value to compute (starts at 1, ends at N)
     */
    static final class GlaishierState {
        final double pi;
        final double sumSoFar;
        final int    nextJ;

        GlaishierState(double pi, double sumSoFar, int nextJ) {
            this.pi      = pi;
            this.sumSoFar = sumSoFar;
            this.nextJ    = nextJ;
        }
    }

    // =========================================================================
    // SeriesService -- all @Task methods
    // =========================================================================

    static class SeriesService {

        private volatile CountDownLatch activeLatch;

        /** Leibniz: 10 groups × 10M terms per group = 100M terms total. */
        private static final long LEIBNIZ_CHUNK = 10_000_000L;

        /** Glaisher: Barnes G sum over N = 10,000 terms, 10 groups × 1,000 each. */
        private static final int GLAISHER_N     = 10_000;
        private static final int GLAISHER_CHUNK = 1_000;

        SeriesService(CountDownLatch latch) { this.activeLatch = latch; }

        void setLatch(CountDownLatch latch) { this.activeLatch = latch; }

        private void done() {
            CountDownLatch l = activeLatch;
            if (l != null) l.countDown();
        }

        // ---------------------------------------------------------------------
        // Entry point
        // ---------------------------------------------------------------------

        /**
         * P pool orchestrator. Submits the Leibniz root producer and exits.
         * The MIRROR chain self-propagates through all 20 downstream stages.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "chainStagger",
                GROUP = 0, ORDER = 0.001)
        public void chainStagger() {
            System.out.println(
                    "  [chainStagger]    P-AEXC   launching Leibniz chain" +
                            " (100M terms, 10 GROUP stages)\n");
            Handler.get().submit("leibnizOp0");
            done();
        }

        // ---------------------------------------------------------------------
        // Leibniz chain -- GROUP 0-9 on I pool
        //
        // Partial Leibniz sums accumulate via MIRROR as Double.
        // leibnizOp0: root producer (no input, CEXC).
        // leibnizOp1-8: mid-chain (receive Double, return Double, IEXC).
        // leibnizOp9: bridge (receives Double, returns GlaishierState, IEXC).
        // ---------------------------------------------------------------------

        /** ROOT producer. GROUP:0. No input. Computes terms k = 0 to 9,999,999. */
        @Task(POOL = "I", EXECUTER = "CEXC", OP = "leibnizOp0",
                GROUP = 0, ORDER = 0.001, MIRROR = "leibnizOp1")
        public Double leibnizOp0() {
            System.out.printf("  [leibnizOp0]      I-CEXC   GROUP:0  terms %,d-%,d%n",
                    0L, LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(0.0, 0);
            done();
            return result;
        }

        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp1",
                GROUP = 1, ORDER = 0.001, MIRROR = "leibnizOp2")
        public Double leibnizOp1(Double accum) {
            System.out.printf("  [leibnizOp1]      I-IEXC   GROUP:1  terms %,d-%,d%n",
                    LEIBNIZ_CHUNK, 2 * LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(accum, 1);
            done();
            return result;
        }

        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp2",
                GROUP = 2, ORDER = 0.001, MIRROR = "leibnizOp3")
        public Double leibnizOp2(Double accum) {
            System.out.printf("  [leibnizOp2]      I-IEXC   GROUP:2  terms %,d-%,d%n",
                    2 * LEIBNIZ_CHUNK, 3 * LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(accum, 2);
            done();
            return result;
        }

        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp3",
                GROUP = 3, ORDER = 0.001, MIRROR = "leibnizOp4")
        public Double leibnizOp3(Double accum) {
            System.out.printf("  [leibnizOp3]      I-IEXC   GROUP:3  terms %,d-%,d%n",
                    3 * LEIBNIZ_CHUNK, 4 * LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(accum, 3);
            done();
            return result;
        }

        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp4",
                GROUP = 4, ORDER = 0.001, MIRROR = "leibnizOp5")
        public Double leibnizOp4(Double accum) {
            System.out.printf("  [leibnizOp4]      I-IEXC   GROUP:4  terms %,d-%,d%n",
                    4 * LEIBNIZ_CHUNK, 5 * LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(accum, 4);
            done();
            return result;
        }

        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp5",
                GROUP = 5, ORDER = 0.001, MIRROR = "leibnizOp6")
        public Double leibnizOp5(Double accum) {
            System.out.printf("  [leibnizOp5]      I-IEXC   GROUP:5  terms %,d-%,d%n",
                    5 * LEIBNIZ_CHUNK, 6 * LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(accum, 5);
            done();
            return result;
        }

        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp6",
                GROUP = 6, ORDER = 0.001, MIRROR = "leibnizOp7")
        public Double leibnizOp6(Double accum) {
            System.out.printf("  [leibnizOp6]      I-IEXC   GROUP:6  terms %,d-%,d%n",
                    6 * LEIBNIZ_CHUNK, 7 * LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(accum, 6);
            done();
            return result;
        }

        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp7",
                GROUP = 7, ORDER = 0.001, MIRROR = "leibnizOp8")
        public Double leibnizOp7(Double accum) {
            System.out.printf("  [leibnizOp7]      I-IEXC   GROUP:7  terms %,d-%,d%n",
                    7 * LEIBNIZ_CHUNK, 8 * LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(accum, 7);
            done();
            return result;
        }

        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp8",
                GROUP = 8, ORDER = 0.001, MIRROR = "leibnizOp9")
        public Double leibnizOp8(Double accum) {
            System.out.printf("  [leibnizOp8]      I-IEXC   GROUP:8  terms %,d-%,d%n",
                    8 * LEIBNIZ_CHUNK, 9 * LEIBNIZ_CHUNK - 1);
            Double result = leibnizChunk(accum, 8);
            done();
            return result;
        }

        /**
         * BRIDGE. GROUP:9. Final Leibniz stage and first Glaisher stage.
         *
         * Receives accumulated partial sum (Double) from leibnizOp8.
         * Computes the last 10M Leibniz terms, multiplies by 4 to get PI.
         * Returns GlaishierState wrapping PI and initialising the Glaisher sum.
         *
         * This is a mid-chain node: Double in, GlaishierState out.
         * seal() validates both edges:
         *   input:  Double         ← Double         (from leibnizOp8)
         *   output: GlaishierState ← GlaishierState (to glashierOp0)
         * The type boundary is explicit, seal-proven, and reported on mismatch.
         */
        @Task(POOL = "I", EXECUTER = "IEXC", OP = "leibnizOp9",
                GROUP = 9, ORDER = 0.001, MIRROR = "glashierOp0")
        public GlaishierState leibnizOp9(Double accum) {
            double partial = leibnizChunk(accum, 9);
            double pi      = 4.0 * partial;

            System.out.printf("%n  [leibnizOp9]      I-IEXC   GROUP:9 complete%n");
            System.out.printf("                    PI = %.10f  (true: 3.1415926536)%n%n", pi);
            System.out.println("  Handing PI to Glaisher chain -> GROUP stages restart on P pool\n");

            done();
            return new GlaishierState(pi, 0.0, 1);
        }

        // ---------------------------------------------------------------------
        // Glaisher chain -- GROUP 0-9 on P pool
        //
        // Accumulates Barnes G sum: Σ(j=1..N) (N−j+1)·ln(j) via MIRROR.
        // glashierOp0-8: mid-chain nodes (receive + return GlaishierState, AEXC).
        // glashierOp9: terminal consumer -- applies correction and reports A.
        // ---------------------------------------------------------------------

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp0",
                GROUP = 0, ORDER = 0.001, MIRROR = "glashierOp1")
        public GlaishierState glashierOp0(GlaishierState state) {
            System.out.printf("  [glashierOp0]     P-AEXC   GROUP:0  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp1",
                GROUP = 1, ORDER = 0.001, MIRROR = "glashierOp2")
        public GlaishierState glashierOp1(GlaishierState state) {
            System.out.printf("  [glashierOp1]     P-AEXC   GROUP:1  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp2",
                GROUP = 2, ORDER = 0.001, MIRROR = "glashierOp3")
        public GlaishierState glashierOp2(GlaishierState state) {
            System.out.printf("  [glashierOp2]     P-AEXC   GROUP:2  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp3",
                GROUP = 3, ORDER = 0.001, MIRROR = "glashierOp4")
        public GlaishierState glashierOp3(GlaishierState state) {
            System.out.printf("  [glashierOp3]     P-AEXC   GROUP:3  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp4",
                GROUP = 4, ORDER = 0.001, MIRROR = "glashierOp5")
        public GlaishierState glashierOp4(GlaishierState state) {
            System.out.printf("  [glashierOp4]     P-AEXC   GROUP:4  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp5",
                GROUP = 5, ORDER = 0.001, MIRROR = "glashierOp6")
        public GlaishierState glashierOp5(GlaishierState state) {
            System.out.printf("  [glashierOp5]     P-AEXC   GROUP:5  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp6",
                GROUP = 6, ORDER = 0.001, MIRROR = "glashierOp7")
        public GlaishierState glashierOp6(GlaishierState state) {
            System.out.printf("  [glashierOp6]     P-AEXC   GROUP:6  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp7",
                GROUP = 7, ORDER = 0.001, MIRROR = "glashierOp8")
        public GlaishierState glashierOp7(GlaishierState state) {
            System.out.printf("  [glashierOp7]     P-AEXC   GROUP:7  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp8",
                GROUP = 8, ORDER = 0.001, MIRROR = "glashierOp9")
        public GlaishierState glashierOp8(GlaishierState state) {
            System.out.printf("  [glashierOp8]     P-AEXC   GROUP:8  j=%,d..%,d%n",
                    state.nextJ, state.nextJ + GLAISHER_CHUNK - 1);
            GlaishierState result = glaishierChunk(state);
            done();
            return result;
        }

        /**
         * TERMINAL consumer. GROUP:9.
         *
         * Receives GlaishierState with S accumulated over j=1..9,000.
         * Computes the final chunk (j=9,001..10,000) then applies the
         * Barnes G asymptotic correction to extract ln(A):
         *
         *   ln(A) = S − (N²/2 + N/2)·ln(N) + 3N²/4 − N/2·ln(2π) + 1/12·ln(N)
         *
         * π in ln(2π) is the value received from the Leibniz chain. Leibniz
         * approximation error propagates here -- this is by design.
         * Expected accuracy: ~6 significant digits at N=10,000.
         */
        @Task(POOL = "P", EXECUTER = "AEXC", OP = "glashierOp9",
                GROUP = 9, ORDER = 0.001)
        public void glashierOp9(GlaishierState state) {
            System.out.printf("  [glashierOp9]     P-AEXC   GROUP:9  j=%,d..%,d  (final)%n",
                    state.nextJ, GLAISHER_N);

            // Compute last chunk: j = state.nextJ to GLAISHER_N
            double sum = state.sumSoFar;
            for (int j = state.nextJ; j <= GLAISHER_N; j++) {
                sum += (double)(GLAISHER_N - j + 1) * Math.log(j);
            }

            // Barnes G asymptotic for z = N+1 (since we want G((N+1)+1) = G(N+2)):
            //   ln G(z+1) = z²/2·ln(z) - 3z²/4 + z/2·ln(2pi) - 1/12·ln(z) + 1/12 - ln(A)
            //   rearranged: ln(A) = correction - sum   <-- note: sum is subtracted, not added
            double n1  = GLAISHER_N + 1.0;
            double ln1 = Math.log(n1);
            double lnA = n1*n1/2.0 * ln1
                    - 3.0*n1*n1/4.0
                    + n1/2.0 * Math.log(2.0 * state.pi)
                    - 1.0/12.0 * ln1
                    + 1.0/12.0
                    - sum;
            double A = Math.exp(lnA);

            System.out.printf("%n");
            System.out.printf("  +------ Series Results -----------------------------------------------+%n");
            System.out.printf("  |  PI (Leibniz, 100M terms):  %.10f                          |%n", state.pi);
            System.out.printf("  |  PI (reference):             3.1415926536                       |%n");
            System.out.printf("  |                                                                   |%n");
            System.out.printf("  |  A  (Glaisher, N=10,000):   %.10f                          |%n", A);
            System.out.printf("  |  A  (reference):             1.2824271291                       |%n");
            System.out.printf("  |                                                                   |%n");
            System.out.printf("  |  ln(2*pi) used PI from Leibniz chain                             |%n");
            System.out.printf("  |  Leibniz error propagates into A -- by design                    |%n");
            System.out.printf("  +-------------------------------------------------------------------+%n");

            done();
        }

        // ---------------------------------------------------------------------
        // Private helpers
        // ---------------------------------------------------------------------

        /**
         * Computes one chunk of the Leibniz partial sum.
         *
         * groupIndex: which of the 10 groups (0-9).
         * k range: groupIndex × LEIBNIZ_CHUNK  to  (groupIndex+1) × LEIBNIZ_CHUNK − 1
         * Contribution: Σ(k in range) (−1)^k / (2k+1)
         */
        private static double leibnizChunk(double accum, int groupIndex) {
            long   start = (long) groupIndex * LEIBNIZ_CHUNK;
            long   end   = start + LEIBNIZ_CHUNK;
            double sum   = accum;
            for (long k = start; k < end; k++) {
                double term = 1.0 / (2.0 * k + 1.0);
                sum += (k % 2L == 0L) ? term : -term;
            }
            return sum;
        }

        /**
         * Computes one chunk of the Barnes G sum.
         *
         * Adds (N−j+1)·ln(j) for j = state.nextJ to state.nextJ + GLAISHER_CHUNK − 1.
         * Returns a new GlaishierState with sumSoFar and nextJ advanced by one chunk.
         */
        private static GlaishierState glaishierChunk(GlaishierState state) {
            int    start = state.nextJ;
            int    end   = start + GLAISHER_CHUNK - 1;
            double sum   = state.sumSoFar;
            for (int j = start; j <= end; j++) {
                sum += (double)(GLAISHER_N - j + 1) * Math.log(j);
            }
            return new GlaishierState(state.pi, sum, end + 1);
        }
    }

    // =========================================================================
    // Main
    // =========================================================================

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== jv-guard GroupSeries ===");
        System.out.println("    Leibniz -> PI (100M terms) via MIRROR chain (GROUP 0-9, I pool)");
        System.out.println("    Barnes G -> Glaisher A (N=10k) via MIRROR chain (GROUP 0-9, P pool)");
        System.out.println("    21 tasks, 20 MIRROR edges, 2 constants, PI feeds Glaisher\n");

        SeriesService service = new SeriesService(null);

        System.out.println(">> registering...");
        Wrapping.register(service);

        System.out.println(">> sealing...\n");
        Wrapping.seal();

        //   I pool (3 cores): leibnizOp0 (CEXC), leibnizOp1-9 (IEXC)
        //   P pool (1 core):  chainStagger, glashierOp0-9 (AEXC)
        //   H pool (3 cores): unused in this demo
        //   R pool (1 core):  unused in this demo
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

        // latch(21): chainStagger + leibnizOp0-9 + glashierOp0-9
        CountDownLatch latch = new CountDownLatch(21);
        service.setLatch(latch);

        System.out.println(">> submitting chainStagger...\n");
        long startNs = System.nanoTime();
        Handler.get().submit("chainStagger");

        boolean ok = latch.await(60, TimeUnit.SECONDS);
        double  elapsed = (System.nanoTime() - startNs) / 1_000_000_000.0;

        System.out.printf("%n>> %s  (%.2fs elapsed)%n%n",
                ok ? "all 21 tasks completed" : "TIMEOUT -- check logs above", elapsed);

        System.out.println(">> shutting down...");
        Executers.shutdown();
        Pools.getRegistry().shutdown();
        System.out.println("\n=== done ===");
    }
}