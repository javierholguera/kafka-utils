package com.javierholguera.utils.streams.topology;

import org.apache.kafka.streams.StreamsBuilder;

/**
 * Describes a Kafka Streams topology using either DSL or Processor API.
 */
public interface TopologyDescriptor {

  /**
   * Configures the topology steps.
   *
   * @param streamsBuilder receives DSL and Processor API instructions to describe the topology.
   */
  void configure(StreamsBuilder streamsBuilder);
}
