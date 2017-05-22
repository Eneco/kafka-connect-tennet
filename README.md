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
| tennet.bidladder.total.topic   | No       | - | The topic to write bidladder total data to.        |
| tennet.balance.delta.topic | No | - | The topic to write balance delta data to. |
| tennet.settled.rrp.topic   | No       | -  | The topic to write settled rrp data to.       |
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
tennet.imbalance.topic = tennet_imbalance
tennet.setttlement.prices.topic = tennet_settlement_prices
tennet.bid.ladder.topic = tennet_bid_ladder
tennet.bid.ladder.total.topic = tennet_bid_ladder_total
tennet.balance.delta.topic = tennet_balance_delta
tennet.price.ladder.topic = tennet_price_ladder
tennet.settled.rrp.topic = tennet_settled_rrp
```

Post in the config to Connect with DataMountaineers [CLI](https://github.com/datamountaineer/kafka-connect-tools).

```bash
cli.sh create tennet < src/main/resources/connector.properties
```
