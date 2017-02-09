package com.eneco.trading.kafka.connect.tennet

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

// abstraction for configs
class TennetSourceConfig(props: util.Map[String, String]) extends AbstractConfig(TennetSourceConfig.config, props)

object TennetSourceConfig {

  val BALANCE_DELTA_TOPIC = "tennet.balance.delta.topic"
  val BALANCE_DELTA_TOPIC_DOC = "The topic to write imbalance tennet data to."

  val IMBALANCE_TOPIC = "tennet.imbalance.topic"
  val IMBALANCE_TOPIC_DOC = "The topic to write settlement price tennet data to."

  val BID_LADDER_TOPIC = "tennet.bid.ladder.topic"
  val BID_LADDER_DOC = "The topic to write bid ladder tennet data to."

  val BID_LADDER_TOTAL_TOPIC = "tennet.bid.ladder.total.topic"
  val BID_LADDER_TOTAL_DOC = "The topic to write total bid ladder tennet data to."

  val URL = "tennet.url"
  val URL_DOC = "Tennet endpoint"
  val URL_DEFAULT = "http://www.tennet.org/xml/"

  val REFRESH_RATE = "tennet.refresh"
  val REFRESH_RATE_DEFAULT = "PT1M"
  val REFRESH_RATE_DOC = "How often the ftp server is polled; ISO8601 duration"

  val MAX_BACK_OFF = "tennet.max.backoff"
  val MAX_BACK_OFF_DEFAULT = "PT15M"
  val MAX_BACK_OFF_DOC = "On failure, exponentially backoff to at most this ISO8601 duration"


  val config: ConfigDef = new ConfigDef()
    .define(BALANCE_DELTA_TOPIC, Type.STRING, Importance.HIGH, BALANCE_DELTA_TOPIC_DOC)
    .define(IMBALANCE_TOPIC, Type.STRING,Importance.HIGH, IMBALANCE_TOPIC_DOC)
    .define(BID_LADDER_TOPIC, Type.STRING,Importance.HIGH, BID_LADDER_DOC)
    .define(BID_LADDER_TOTAL_TOPIC, Type.STRING,Importance.HIGH, BID_LADDER_TOTAL_DOC)
    .define(URL, Type.STRING, URL_DEFAULT, Importance.HIGH, URL_DOC)
    .define(REFRESH_RATE, Type.STRING, REFRESH_RATE_DEFAULT, Importance.LOW, REFRESH_RATE_DOC)
    .define(MAX_BACK_OFF, Type.STRING, MAX_BACK_OFF_DEFAULT , Importance.LOW, MAX_BACK_OFF_DOC)
}

