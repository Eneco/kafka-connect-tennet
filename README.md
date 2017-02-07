[![Build Status](https://travis-ci.org/Eneco/kafka-connect-tennet.svg?branch=master)](https://travis-ci.org/Eneco/kafka-connect-tennet)

# kafka-connect-tennet
================

Reads the imbalance data (XML) form the tennet website and push it to Kafka

## Configurations

| Name                     | Optional | Default | Description                                  |
|--------------------------|----------|---------|----------------------------------------------|
| tennet.imbalance.topic   | No       | - | The topic to write imbalance data to.        |
| tennet.setttlement.prices.topic | No | - | The topic to write settlement price data to. |
| tennet.bidladder.topic   | No       | -  | The topic to write bid ladder data to.       |
| tennet.url               | Yes      | http://www.tennet.org/xml/balancedeltaprices/balans-delta_2h.xml | Tennet endpoint.|
| tennet.refresh           | Yes      | PT5M | The poll frequency in ISO8601 format.        |
| tennet.max.backoff       | Yes      | PT40M | On failure, exponentially backoff to at most this ISO8601 duration. |


## Adding the Connector to the Classpath

The use the Connector it needs to be on the classpath of your Kafka Connect Cluster. The easiest way is to
explicitly add it like this and then start Kafka Connect.

```bash
export CLASSPATH=target/kafka-connect-tennet-1.0.1-3.0.1-all.jar
```


### Example

```bash
connector.class = com.eneco.trading.kafka.connect.tennet.TennetSourceConnector
tasks.max = 1
name = tennet
tennet.imbalance.topic = imbalance
tennet.setttlement.topic = settlement_prices
tennet.bidladder.topic = bid_ladder
```

Post in the config to Connect with DataMountaineers [CLI](https://github.com/datamountaineer/kafka-connect-tools).

```bash
cli.sh create tennet < src/main/resources/connector.properties
```

Work in progress

- connector reads the XML and creates a message for each record

WIP:
- partitioning/offset/keys

TODO:

- write tests
- more error handling
