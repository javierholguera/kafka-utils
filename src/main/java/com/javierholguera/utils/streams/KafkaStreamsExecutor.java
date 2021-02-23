package com.javierholguera.utils.streams;


import com.javierholguera.utils.streams.topology.TopologyDescriptor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Executes a topology using Kafka Streams.
 */
public class KafkaStreamsExecutor implements StreamsExecutor {

  private static final int CLOSE_TIMEOUT_SECS = 30;
  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsExecutor.class);

  private final Topology topology;
  private final Properties config;
  private final Time clock;

  private KafkaStreams streams;
  private State state;
  private StateListener stateListener;
  private UncaughtExceptionHandler uncaughtExceptionHandler;

  public KafkaStreamsExecutor(Properties config, TopologyDescriptor nodeTopology) {
    this(config, nodeTopology, Time.SYSTEM);
  }

  public KafkaStreamsExecutor(Properties config, TopologyDescriptor nodeTopology, Time clock) {
    //checkNotNull(config, "config must not be null");
    //checkNotNull(nodeTopology, "nodeTopology must not be null");

    this.config = config;
    StreamsBuilder builder = new StreamsBuilder();
    nodeTopology.configure(builder);
    this.topology = builder.build();
    this.clock = clock;
  }

  private static String prettyPrintThreadMetadata(Set<ThreadMetadata> threadsMetadata) {
    StringBuilder sb = new StringBuilder();

    Consumer<Set<TaskMetadata>> prettyFormatTasksMetadata = tasksMetadata -> {
      for (TaskMetadata taskMetadata : tasksMetadata) {
        sb.append("\t\t\ttaskId=").append(taskMetadata.taskId())
            .append(", topicPartitions=").append(taskMetadata.topicPartitions()).append("\n");
      }
    };

    for (ThreadMetadata threadMetadata : threadsMetadata) {
      sb.append("\t").append(threadMetadata.threadName()).append(":\n")
          .append("\t\tthreadState: ").append(threadMetadata.threadState()).append("\n")
          .append("\t\tactiveTasks:\n");
      prettyFormatTasksMetadata.accept(threadMetadata.activeTasks());
      sb.append("\t\tstandbyTasks:\n");
      prettyFormatTasksMetadata.accept(threadMetadata.standbyTasks());
    }
    return sb.toString();
  }

  /**
   * @param shouldCleanLocalStateStores If true, KafkaStreams#cleanUp() will be called before starting the application.
   *                                    This will purge local state store data for the application id.
   */
  @Override
  public synchronized void start(boolean shouldCleanLocalStateStores) {
    //checkState(streams == null, "Attempt to start an already started node");

    logger.info("[{}] Streams node starting with topology:\n{}", getApplicationId(), topology.describe());
    streams = new KafkaStreams(topology, config, clock);
    streams.setStateListener((newState, oldState) -> {
      final State initialState = state != null ? state : State.CREATED;

      // The lifecycle dictates that the RUNNING state will be entered before the first initial REBALANCING.
      // For our intents and purposes, this running state is as good as the last state so only update the state to
      // RUNNING if the old state was REBALANCING.
      if (oldState == State.REBALANCING && newState == State.RUNNING) {
        logger.info("[{}] Streams node state transition from {} to {}\nLocal threads metadata:\n{}",
            getApplicationId(),
            oldState,
            newState,
            prettyPrintThreadMetadata(streams.localThreadsMetadata()));
        state = newState;
      } else if (newState != State.RUNNING) {
        state = newState;
      } else {
        return;
      }

      if (state != initialState && stateListener != null) {
        stateListener.onChange(getApplicationId(), initialState, state);
      }
    });

    if (uncaughtExceptionHandler != null) {
      streams.setUncaughtExceptionHandler(
          (thread, throwable) -> uncaughtExceptionHandler.handle(getApplicationId(), thread, throwable));
    }

    if (shouldCleanLocalStateStores) {
      streams.cleanUp();
    }

    streams.start();

    logger.info("[{}] Streams node started", getApplicationId());
  }

  @Override
  public void setStateListener(StateListener listener) {
    stateListener = listener;
  }

  @Override
  public void setUncaughtExceptionHandler(UncaughtExceptionHandler handler) {
    uncaughtExceptionHandler = handler;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public <K, V> ReadOnlyKeyValueStore<K, V> getStore(String storeName) {
    return streams.store(storeName, QueryableStoreTypes.keyValueStore());
  }

  @Override
  public synchronized void stop() {
    if (streams == null) {
      return;
    }

    logger.info("[{}] Streams node stopping", getApplicationId());

    streams.close(CLOSE_TIMEOUT_SECS, TimeUnit.SECONDS);
    streams = null;

    logger.info("[{}] Streams node stopped", getApplicationId());
  }

  private String getApplicationId() {
    return config.getProperty(StreamsConfig.APPLICATION_ID_CONFIG, "");
  }
}
