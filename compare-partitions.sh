#!/bin/bash

for i in {0..24}; do
  kafkacat -b $PROD_KAFKA_BROKERS -e -t connect-offsets -f '%T - %k - %s\n' -p $i > /tmp/prod/connect-offsets/partition-$i.txt
done
for i in {0..24}; do
  kafkacat -b $PROD_KAFKA_BROKERS -e -t connect-non-critical-offsets -f '%T - %k - %s\n' -p $i > /tmp/prod/connect-non-critical-offsets/partition-$i.txt
done
