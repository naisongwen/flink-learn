package org.learn.flink.savepoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.learn.flink.common.MiniClusterResourceConfiguration;
import org.learn.flink.common.MiniClusterWithClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class RestoreWithModifiedStatelessOperatorsTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(RestoreWithModifiedStatelessOperatorsTest.class);

  public static void main(String[] args) throws Exception {
    File savepointDir = new File("savepoints");
    // Config
    int numTaskManagers = 2;
    int numSlotsPerTaskManager = 2;
    int parallelism = 2;

    // Test deadline
    final Deadline deadline = Deadline.now().plus(Duration.ofMinutes(5));

    // Flink configuration
    final Configuration config = new Configuration();
    config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

    String savepointPath = "C:\\savePointDir\\savepoint-d7ce73-2ab6ac3fab82";

    LOG.info("Flink configuration: " + config + ".");

    // Start Flink
//    MiniClusterWithClientResource cluster =
//        new MiniClusterWithClientResource(
//            new MiniClusterResourceConfiguration.Builder()
//                .setConfiguration(config)
//                .setNumberTaskManagers(numTaskManagers)
//                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
//                .build());

//    LOG.info("Shutting down Flink cluster.");
//    cluster.before();
//    ClusterClient<?> client = cluster.getClusterClient();
//    try {
//      final StatefulCounter statefulCounter = new StatefulCounter();
//      StatefulCounter.resetForTest(parallelism);
//
//      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      env.setParallelism(parallelism);
//      env.addSource(new InfiniteTestSource())
//          .shuffle()
//          .map(value -> 4 * value)
//          .shuffle()
//          .map(statefulCounter)
//          .uid("statefulCounter")
//          .shuffle()
//          .map(value -> 2 * value)
//          .addSink(new DiscardingSink<>());
//
//      JobGraph originalJobGraph = env.getStreamGraph().getJobGraph();
//
//      JobID jobID = client.submitJob(originalJobGraph).get();
//
//      // wait for the Tasks to be ready
//      assertTrue(
//          StatefulCounter.getProgressLatch()
//              .await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));
//
//      savepointPath = client.triggerSavepoint(jobID, null).get();
//      LOG.info("Retrieved savepoint: " + savepointPath + ".");
//    } finally {
//      // Shut down the Flink cluster (thereby canceling the job)
//      LOG.info("Shutting down Flink cluster.");
//      cluster.after();
//    }

    // create a new MiniCluster to make sure we start with completely
    // new resources
    MiniClusterWithClientResource cluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(config)
                .setNumberTaskManagers(numTaskManagers)
                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                .build());
    LOG.info("Restarting Flink cluster.");
    cluster.before();
    ClusterClient<?> client = cluster.getClusterClient();
    try {
      // Reset static test helpers
      StatefulCounter.resetForTest(parallelism);

      // Gather all task deployment descriptors
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(parallelism);

      // generate a modified job graph that adds a stateless op
      env.addSource(new InfiniteTestSource())
          .shuffle()
          .map(new StatefulCounter())
          .uid("statefulCounter")
          .shuffle()
          .map(value -> value)
          .addSink(new DiscardingSink<>());

      JobGraph modifiedJobGraph = env.getStreamGraph().getJobGraph();

      // Set the savepoint path
      modifiedJobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

      LOG.info(
          "Resubmitting job "
              + modifiedJobGraph.getJobID()
              + " with "
              + "savepoint path "
              + savepointPath
              + " in detached mode.");

      // Submit the job
      client.submitJob(modifiedJobGraph).get();
      // Await state is restored
      assertTrue(
          StatefulCounter.getRestoreLatch()
              .await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));

      // Await some progress after restore
      assertTrue(
          StatefulCounter.getProgressLatch()
              .await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));
    } finally {
      cluster.after();
    }
  }

  private static class InfiniteTestSource implements SourceFunction<Integer> {

    private static final long serialVersionUID = 1L;
    private volatile boolean running = true;
    private volatile boolean suspended = false;
    private static final Collection<InfiniteTestSource> createdSources =
        new CopyOnWriteArrayList<>();
    private transient volatile CompletableFuture<Void> completeFuture;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
      completeFuture = new CompletableFuture<>();
      createdSources.add(this);
      try {
        while (running) {
          if (!suspended) {
            synchronized (ctx.getCheckpointLock()) {
              ctx.collect(1);
            }
          }
          Thread.sleep(1);
        }
        completeFuture.complete(null);
      } catch (Exception e) {
        completeFuture.completeExceptionally(e);
        throw e;
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    public void suspend() {
      suspended = true;
    }

    public static void resetForTest() {
      createdSources.clear();
    }

    public CompletableFuture<Void> getCompleteFuture() {
      return completeFuture;
    }

//    public static void cancelAllAndAwait() throws ExecutionException, InterruptedException {
//      createdSources.forEach(InfiniteTestSource::cancel);
//      allOf(
//              createdSources.stream()
//                  .map(InfiniteTestSource::getCompleteFuture)
//                  .toArray(CompletableFuture[]::new))
//          .get();
//    }

    public static void suspendAll() {
      createdSources.forEach(InfiniteTestSource::suspend);
    }
  }

  /**
   * An {@link InfiniteTestSource} implementation that fails when cancel is called for the first
   * time.
   */
  private static class CancelFailingInfiniteTestSource extends InfiniteTestSource {

    private static volatile boolean cancelTriggered = false;

    @Override
    public void cancel() {
      if (!cancelTriggered) {
        cancelTriggered = true;
        throw new RuntimeException("Expected RuntimeException after snapshot creation.");
      }
      super.cancel();
    }
  }

  /** An {@link InfiniteTestSource} implementation that fails while creating a snapshot. */
  private static class SnapshotFailingInfiniteTestSource extends InfiniteTestSource
      implements CheckpointedFunction {

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      throw new Exception(
          "Expected Exception happened during snapshot creation within test source");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      // all good here
    }
  }

  private static class StatefulCounter extends RichMapFunction<Integer, Integer>
      implements ListCheckpointed<byte[]> {

    private static volatile CountDownLatch progressLatch = new CountDownLatch(0);
    private static volatile CountDownLatch restoreLatch = new CountDownLatch(0);

    private int numCollectedElements = 0;

    private static final long serialVersionUID = 7317800376639115920L;
    private byte[] data;

    @Override
    public void open(Configuration parameters) throws Exception {
      if (data == null) {
        // We need this to be large, because we want to test with files
        Random rand = new Random(getRuntimeContext().getIndexOfThisSubtask());
        data =
            new byte
                [(int) CheckpointingOptions.FS_SMALL_FILE_THRESHOLD.defaultValue().getBytes() + 1];
        rand.nextBytes(data);
      }
    }

    @Override
    public Integer map(Integer value) throws Exception {
      for (int i = 0; i < data.length; i++) {
        data[i] += 1;
      }

      if (numCollectedElements++ > 10) {
        progressLatch.countDown();
      }

      return value;
    }

    @Override
    public List<byte[]> snapshotState(long checkpointId, long timestamp) throws Exception {
      return Collections.singletonList(data);
    }

    @Override
    public void restoreState(List<byte[]> state) throws Exception {
      if (state.isEmpty() || state.size() > 1) {
        throw new RuntimeException(
            "Test failed due to unexpected recovered state size " + state.size());
      }
      this.data = state.get(0);

      restoreLatch.countDown();
    }

    // --------------------------------------------------------------------

    static CountDownLatch getProgressLatch() {
      return progressLatch;
    }

    static CountDownLatch getRestoreLatch() {
      return restoreLatch;
    }

    static void resetForTest(int parallelism) {
      progressLatch = new CountDownLatch(parallelism);
      restoreLatch = new CountDownLatch(parallelism);
    }
  }
}
