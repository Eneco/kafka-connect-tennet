package com.eneco.trading.kafka.connect.tennet

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

// abstraction for configs
class TennetSourceConfig(props: util.Map[String, String]) extends AbstractConfig(TennetSourceConfig.config, props)

object TennetSourceConfig {

  val TOPIC = "tennet.topic"
  val TOPIC_DOC = "The topic to write tennent data to."
  val URL = "tennet.url"
  val URL_DOC = "Tennet imbalance endpoint"
  val URL_DEFAULT = "http://www.tennet.org/xml/balancedeltaprices/balans-delta_2h.xml"

  val REFRESH_RATE = "tennet.refresh"
  val REFRESH_RATE_DEFAULT = "PT5M"
  val REFRESH_RATE_DOC = "How often the ftp server is polled; ISO8601 duration"

  val MAX_BACK_OFF = "tennet.max.backoff"
  val MAX_BACK_OFF_DEFAULT = "PT40M"
  val MAX_BACK_OFF_DOC = "On failure, exponentially backoff to at most this ISO8601 duration"


  val config: ConfigDef = new ConfigDef()
    .define(TOPIC, Type.STRING,Importance.HIGH, TOPIC_DOC)
    .define(URL, Type.STRING, URL_DEFAULT, Importance.HIGH, URL_DOC)
    .define(REFRESH_RATE, Type.STRING, REFRESH_RATE_DEFAULT, Importance.LOW, REFRESH_RATE_DOC)
    .define(MAX_BACK_OFF, Type.STRING, MAX_BACK_OFF_DEFAULT , Importance.LOW, MAX_BACK_OFF_DOC)
}

