package com.javierholguera.utils.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public interface StreamsExecutor {

  interface StateListener {
    void onChange(String appId, KafkaStreams.State newState, final KafkaStreams.State oldState);
  }

  interface UncaughtExceptionHandler {
    void handle(String applicationId, Thread t, Throwable e);
  }

  void start(boolean shouldCleanLocalStateStores);
  void setStateListener(StateListener listener);
  void setUncaughtExceptionHandler(UncaughtExceptionHandler handler);
  KafkaStreams.State getState();
  <K, V> ReadOnlyKeyValueStore<K, V> getStore(String storeName);
  void stop();
}