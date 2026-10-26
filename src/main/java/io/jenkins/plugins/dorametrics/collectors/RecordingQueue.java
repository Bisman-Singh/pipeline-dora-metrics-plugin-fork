package io.jenkins.plugins.dorametrics.collectors;

import hudson.init.Terminator;
import hudson.model.Run;
import hudson.security.ACL;
import hudson.security.ACLContext;
import hudson.util.DaemonThreadFactory;
import hudson.util.NamingThreadFactory;
import io.jenkins.plugins.dorametrics.store.MetricsStore;
import jenkins.util.SystemProperties;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Writes finished builds to the store on one background thread, so a build waits on the database
 * for a few seconds at most. Jenkins counts a build as running until every completion listener
 * returns, so a write stuck on slow storage would keep the build running, and the next build of
 * a job that does not allow concurrent builds waiting behind it.
 *
 * <p>A build waits for its own write for up to {@link #WAIT_PROPERTY} seconds, so on healthy
 * storage the data is there when the build finishes, as before. Once the write in progress has
 * taken longer than that, later builds stop waiting at all. Builds queue in order; when
 * {@link #CAPACITY_PROPERTY} builds are already waiting, a build is not recorded and the log says
 * so. Import history adds any build that was not recorded.
 */
public final class RecordingQueue {

    static final String WAIT_PROPERTY = RecordingQueue.class.getName() + ".waitSeconds";
    static final String CAPACITY_PROPERTY = RecordingQueue.class.getName() + ".capacity";

    private static final Logger LOGGER = Logger.getLogger(RecordingQueue.class.getName());

    /** What records a build. Tests swap it for one that blocks. */
    static Consumer<Run<?, ?>> recorder = BuildRecorder::record;

    private static ThreadPoolExecutor executor;
    /** When the write in progress started, or 0 when none is. */
    private static volatile long busySince;

    private RecordingQueue() {
    }

    /** Hands {@code run} to the writer and waits a little while for it, never longer. */
    static void record(Run<?, ?> run) {
        ThreadPoolExecutor pool = executor();
        Future<?> written;
        try {
            written = pool.submit(() -> write(run));
        } catch (RejectedExecutionException e) {
            LOGGER.warning("Pipeline DORA Metrics: not recording " + run.getFullDisplayName() + ", "
                    + pool.getQueue().size() + " builds are already waiting to be written. "
                    + "Import history on the dashboard adds it later.");
            return;
        }

        long waitMs = TimeUnit.SECONDS.toMillis(Math.max(0, SystemProperties.getInteger(WAIT_PROPERTY, 5)));
        long since = busySince;
        if (since != 0 && System.currentTimeMillis() - since > waitMs) {
            LOGGER.fine("Pipeline DORA Metrics: the writer is behind, " + run.getFullDisplayName()
                    + " is written in the background");
            return;
        }
        try {
            written.get(waitMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOGGER.warning("Pipeline DORA Metrics: writing " + run.getFullDisplayName() + " took longer than "
                    + TimeUnit.MILLISECONDS.toSeconds(waitMs) + "s, so the build finishes without waiting for it. "
                    + "Builds are written in the background meanwhile; slow storage for the database is the usual cause.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException e) {
            LOGGER.log(Level.WARNING, "Failed to collect metrics for " + run.getFullDisplayName(), e.getCause());
        }
    }

    private static void write(Run<?, ?> run) {
        busySince = System.currentTimeMillis();
        try (ACLContext ignored = ACL.as2(ACL.SYSTEM2)) {
            recorder.accept(run);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Failed to collect metrics for " + run.getFullDisplayName(), e);
        } finally {
            busySince = 0;
        }
    }

    private static synchronized ThreadPoolExecutor executor() {
        if (executor == null || executor.isShutdown()) {
            int capacity = Math.max(1, SystemProperties.getInteger(CAPACITY_PROPERTY, 1000));
            executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(capacity),
                    new NamingThreadFactory(new DaemonThreadFactory(), "Pipeline DORA Metrics recorder"));
        }
        return executor;
    }

    /** Writes what is already queued when Jenkins stops, for a few seconds at most, then closes the store. */
    @Terminator
    public static void shutdown() throws InterruptedException {
        ThreadPoolExecutor pool;
        synchronized (RecordingQueue.class) {
            pool = executor;
            executor = null;
        }
        if (pool != null) {
            pool.shutdown();
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                int left = pool.shutdownNow().size();
                LOGGER.warning("Pipeline DORA Metrics: " + left + " builds were not written before Jenkins stopped. "
                        + "Import history on the dashboard adds them later.");
            }
        }
        MetricsStore.closeOnShutdown();
    }
}
