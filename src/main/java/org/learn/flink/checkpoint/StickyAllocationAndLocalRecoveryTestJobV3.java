/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.learn.flink.checkpoint;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.client.program.PerJobMiniClusterFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

public class StickyAllocationAndLocalRecoveryTestJobV3 {
  static MiniCluster miniCluster;

  private static PerJobMiniClusterFactory initializeMiniCluster() {
    return PerJobMiniClusterFactory.createWithFactory(
        new Configuration(),
        config -> {
          miniCluster = new MiniCluster(config);
          return miniCluster;
        });
  }

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    CheckpointConfig checkpointConfig = env.getCheckpointConfig();
    env.setParallelism(1);
    env.setMaxParallelism(1);
    env.enableCheckpointing(3000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0));

    EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend =
        new EmbeddedRocksDBStateBackend(false);
    embeddedRocksDBStateBackend.setDbStoragePath("file:///c:/rocksdb/");
    env.setStateBackend(embeddedRocksDBStateBackend);
    checkpointConfig.setCheckpointStorage("file:///./checkpointDir/");
    // env.setDefaultSavepointDirectory(savePointPath);
    // 取消作业时是否保留 checkpoint (默认不保留)
    checkpointConfig.enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    env.addSource(new RandomLongSource(3, 10))
        .keyBy((KeySelector<Long, Long>) aLong -> aLong)
        .flatMap(new StateCreatingFlatMap(10))
        .addSink(new PrintSinkFunction<>());

    String savePointPath = "C:\\savePointDir\\savepoint-d7ce73-2ab6ac3fab82/";
    JobGraph modifiedJobGraph = env.getStreamGraph().getJobGraph();

    // Set the savepoint path
    modifiedJobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savePointPath));

    PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();
    JobClient jobClient =
        perJobMiniClusterFactory
            .submitJob(modifiedJobGraph, ClassLoader.getSystemClassLoader())
            .get();
    Thread.sleep(10000);
    // jobClient.triggerSavepoint(savePointPath);
    final JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();
    System.out.println(jobExecutionResult);
  }

  /** Source function that produces a long sequence. */
  private static final class RandomLongSource extends RichParallelSourceFunction<Long>
      implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    /** Generator delay between two events. */
    final long delay;

    /** Maximum restarts before shutting down this source. */
    final int maxAttempts;

    /** State that holds the current key for recovery. */
    transient ListState<Long> sourceCurrentKeyState;

    /** Generator's current key. */
    long currentKey;

    /** Generator runs while this is true. */
    volatile boolean running;

    RandomLongSource(int maxAttempts, long delay) {
      this.delay = delay;
      this.maxAttempts = maxAttempts;
      this.running = true;
    }

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {

      int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
      int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

      // the source emits one final event and shuts down once we have reached max attempts.
      if (getRuntimeContext().getAttemptNumber() > maxAttempts) {
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.collect(Long.MAX_VALUE - subtaskIdx);
        }
        return;
      }

      while (running) {

        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.collect(currentKey);
          currentKey += numberOfParallelSubtasks;
        }

        if (delay > 0) {
          Thread.sleep(delay);
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      sourceCurrentKeyState.clear();
      sourceCurrentKeyState.add(currentKey);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

      ListStateDescriptor<Long> currentKeyDescriptor =
          new ListStateDescriptor<>("currentKey", Long.class);
      sourceCurrentKeyState = context.getOperatorStateStore().getListState(currentKeyDescriptor);

      currentKey = getRuntimeContext().getIndexOfThisSubtask();
      Iterable<Long> iterable = sourceCurrentKeyState.get();
      if (iterable != null) {
        Iterator<Long> iterator = iterable.iterator();
        if (iterator.hasNext()) {
          currentKey = iterator.next();
          Preconditions.checkState(!iterator.hasNext());
        }
      }
    }
  }

  /** Stateful map function. Failure creation and checks happen here. */
  private static final class StateCreatingFlatMap extends RichFlatMapFunction<Long, String>
      implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    /** User configured size of the generated artificial values in the keyed state. */
    final int valueSize;

    /** This state is used to create artificial keyed state in the backend. */
    transient ValueState<String> valueState;

    /** This state is used to persist the schedulingAndFailureInfo to state. */
    transient ListState<MapperSchedulingAndFailureInfo> schedulingAndFailureState;

    /** This contains the current scheduling and failure meta data. */
    transient MapperSchedulingAndFailureInfo currentSchedulingAndFailureInfo;

    /** Message to indicate that recovery detected a failure with sticky allocation. */
    transient volatile String allocationFailureMessage;
    /** If this flag is true, the next invocation of the map function introduces a test failure. */
    transient volatile boolean failTask;

    StateCreatingFlatMap(int valueSize) {
      this.valueSize = valueSize;
      this.failTask = false;
      this.allocationFailureMessage = null;
    }

    @Override
    public void flatMap(Long key, Collector<String> collector) throws IOException {

      // store artificial data to blow up the state
      valueState.update(RandomStringUtils.random(valueSize, true, true));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {}

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
        throws Exception {
      ValueStateDescriptor<String> stateDescriptor =
          new ValueStateDescriptor<>("state", String.class);
      valueState = functionInitializationContext.getKeyedStateStore().getState(stateDescriptor);

      ListStateDescriptor<MapperSchedulingAndFailureInfo> mapperInfoStateDescriptor =
          new ListStateDescriptor<>("mapperState", MapperSchedulingAndFailureInfo.class);
      schedulingAndFailureState =
          functionInitializationContext
              .getOperatorStateStore()
              .getUnionListState(mapperInfoStateDescriptor);

      StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
      String allocationID = runtimeContext.getAllocationIDAsString();
      // Pattern of the name: "Flat Map -> Sink: Unnamed (4/4)#0". Remove "#0" part:
      String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks().split("#")[0];

      schedulingAndFailureState.clear();
      schedulingAndFailureState.add(currentSchedulingAndFailureInfo);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      // we can only fail the task after at least one checkpoint is completed to record
      // progress.
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    /** Records the information required to check sticky scheduling after a restart. */
    public static class MapperSchedulingAndFailureInfo implements Serializable {

      private static final long serialVersionUID = 1L;

      /** Name and subtask index of the task. */
      final String taskNameWithSubtask;

      /** The current allocation id of this task. */
      final String allocationId;

      MapperSchedulingAndFailureInfo(
          boolean failingTask,
          boolean killedJvm,
          int jvmPid,
          String taskNameWithSubtask,
          String allocationId) {

        this.taskNameWithSubtask = taskNameWithSubtask;
        this.allocationId = allocationId;
      }

      @Override
      public String toString() {
        return "MapperTestInfo{"
            + ", taskNameWithSubtask='"
            + taskNameWithSubtask
            + '\''
            + ", allocationId='"
            + allocationId
            + '\''
            + '}';
      }
    }
  }
}
