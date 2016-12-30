package com.eneco.trading.kafka.connect.tennet

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

// abstraction for configs
class TennetSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(TennetSourceConfig.definition, props) {

}
object TennetSourceConfig {

  val Topic = "topic"
  val Url = "url"
  val Interval = "interval"

  val definition: ConfigDef = new ConfigDef()
    .define(Topic,Type.STRING,Importance.HIGH,"Target topic")
    .define(Url,Type.STRING,Importance.HIGH,"Tennet imbalance endpoint")
    .define(Interval,Type.LONG,Importance.HIGH,"Interval between two calls")
}

