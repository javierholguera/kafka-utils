#!/bin/bash

curl $DEV_KAFKA_CONNECT_CRITICAL/connectors | jq -c '.[]' | tr -d \"  | while read i; do
curl $DEV_KAFKA_CONNECT_CRITICAL/connectors/$i/config | jq -c '. | "\(connector.class) \(source.struct.version)"'
done
