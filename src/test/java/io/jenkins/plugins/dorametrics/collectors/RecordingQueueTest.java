package io.jenkins.plugins.dorametrics.collectors;

import hudson.model.FreeStyleBuild;
import hudson.model.FreeStyleProject;
import hudson.model.Run;
import io.jenkins.plugins.dorametrics.store.MetricsStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.LogRecorder;
import org.jvnet.hudson.test.junit.jupiter.WithJenkins;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A build never stays running because its metrics cannot be written: Jenkins counts a build as
 * running until the completion listeners return, and the next build of a job without concurrent
 * builds waits for that.
 */
@WithJenkins
class RecordingQueueTest {

    private JenkinsRule j;
    private Consumer<Run<?, ?>> original;
    private LogRecorder logs;
    private final CountDownLatch release = new CountDownLatch(1);
    private final List<String> started = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp(JenkinsRule rule) {
        j = rule;
        MetricsStore.setInstance(null);
        original = RecordingQueue.recorder;
        logs = new LogRecorder().record(RecordingQueue.class, Level.WARNING).capture(20);
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        RecordingQueue.recorder = original;
        System.clearProperty(RecordingQueue.WAIT_PROPERTY);
        System.clearProperty(RecordingQueue.CAPACITY_PROPERTY);
        logs.close();
    }

    /** A recorder that hangs like a write on stalled storage, until the test lets it go. */
    private void recorderThatHangs() {
        RecordingQueue.recorder = run -> {
            started.add(run.getFullDisplayName());
            try {
                release.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    private void awaitStarted(int count) throws InterruptedException {
        for (int i = 0; i < 100 && started.size() < count; i++) {
            Thread.sleep(100);
        }
    }

    @Test
    void aStuckWriteDoesNotKeepTheBuildRunning() throws Exception {
        System.setProperty(RecordingQueue.WAIT_PROPERTY, "1");
        recorderThatHangs();
        FreeStyleProject app = j.createFreeStyleProject("app");

        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            FreeStyleBuild first = j.buildAndAssertSuccess(app);
            FreeStyleBuild second = j.buildAndAssertSuccess(app);
            assertFalse(first.isBuilding());
            assertFalse(second.isBuilding());
        });
        assertEquals(List.of("app #1"), started, "the second build waits in line behind the stuck write");

        release.countDown();
        awaitStarted(2);
        assertEquals(List.of("app #1", "app #2"), started, "both are written once the storage answers");
    }

    @Test
    void onceTheWriterIsBehindBuildsStopWaiting() throws Exception {
        System.setProperty(RecordingQueue.WAIT_PROPERTY, "8");
        recorderThatHangs();
        FreeStyleProject app = j.createFreeStyleProject("app");

        long t0 = System.nanoTime();
        j.buildAndAssertSuccess(app);
        long first = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        long t1 = System.nanoTime();
        j.buildAndAssertSuccess(app);
        long second = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);

        assertTrue(first >= 8000, "the first build waits for its write, " + first + "ms");
        assertTrue(second < 8000, "the second does not wait behind a write that is already late, " + second + "ms");
        assertTrue(logs.getMessages().stream().anyMatch(m -> m.contains("app #1") && m.contains("longer than 8s")),
                String.valueOf(logs.getMessages()));
    }

    @Test
    void aFullQueueSkipsTheBuildAndSaysSo() throws Exception {
        System.setProperty(RecordingQueue.WAIT_PROPERTY, "0");
        System.setProperty(RecordingQueue.CAPACITY_PROPERTY, "1");
        recorderThatHangs();
        FreeStyleProject app = j.createFreeStyleProject("app");

        j.buildAndAssertSuccess(app);
        awaitStarted(1);
        j.buildAndAssertSuccess(app);
        j.buildAndAssertSuccess(app);

        assertTrue(logs.getMessages().stream().anyMatch(m -> m.contains("not recording app #3")),
                String.valueOf(logs.getMessages()));
        release.countDown();
        awaitStarted(2);
        assertEquals(List.of("app #1", "app #2"), started);
    }

    @Test
    void onHealthyStorageTheBuildIsStoredWhenItFinishes() throws Exception {
        FreeStyleProject app = j.createFreeStyleProject("app");
        j.buildAndAssertSuccess(app);
        assertEquals(1, MetricsStore.getInstance().getBuilds("app", 0, Long.MAX_VALUE).size());
    }
}
