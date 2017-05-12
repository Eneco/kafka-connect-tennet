package com.eneco.trading.kafka.connect.tennet

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}

// abstraction for configs
class TennetSourceConfig(props: util.Map[String, String]) extends AbstractConfig(TennetSourceConfig.config, props)

object TennetSourceConfig {

  val BALANCE_DELTA_TOPIC = "tennet.balance.delta.topic"
  val BALANCE_DELTA_TOPIC_DOC = "The topic to write imbalance deltas tennet data to."

  val IMBALANCE_TOPIC = "tennet.imbalance.topic"
  val IMBALANCE_TOPIC_DOC = "The topic to write imbalance settlement price tennet data to."

  val BID_LADDER_TOPIC = "tennet.bid.ladder.topic"
  val BID_LADDER_DOC = "The topic to write bid ladder tennet data to."

  val BID_LADDER_TOTAL_TOPIC = "tennet.bid.ladder.total.topic"
  val BID_LADDER_TOTAL_DOC = "The topic to write total bid ladder tennet data to."

  val PRICE_LADDER_TOPIC = "tennet.price.ladder.topic"
  val PRICE_LADDER_DOC = "The topic to write price ladder tennet data to."

  val SETTLED_RRP_TOPIC = "tennet.settled.rrp.topic"
  val SETTLED_RRP_DOC = "The topic to write settled rrp volumes to."

  val URL = "tennet.url"
  val URL_DOC = "Tennet endpoint"
  val URL_DEFAULT = "http://www.tennet.org/xml/"

  val REFRESH_RATE = "tennet.refresh"
  val REFRESH_RATE_DEFAULT = "PT1M"
  val REFRESH_RATE_DOC = "How often the ftp server is polled; ISO8601 duration"

  val MAX_BACK_OFF = "tennet.max.backoff"
  val MAX_BACK_OFF_DEFAULT = "PT15M"
  val MAX_BACK_OFF_DOC = "On failure, exponentially backoff to at most this ISO8601 duration"

  val TIMEZONE = "tennet.timezone"
  val TIMEZONE_DEFAULT = "Europe/Amsterdam"
  val TIMEZONE_DOC = "Timezone of the tennet API, used for deriving epochmillis for records"

  val SCHEMA_IMBALANCE = SchemaBuilder.struct().name(IMBALANCE_TOPIC)
    .field("number", Schema.INT64_SCHEMA)
    .field("sequence_number", Schema.INT64_SCHEMA)
    .field("time", Schema.STRING_SCHEMA)
    .field("igcccontribution_up", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("igcccontribution_down", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("upward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("downward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("reserve_upward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("reserve_downward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("incident_reserve_up_indicator", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("incident_reserve_down_indicator", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("min_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("mid_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("max_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("value_time", Schema.INT64_SCHEMA)
    .build()

  val SCHEMA_BIDLADDER = SchemaBuilder.struct().name(BID_LADDER_TOPIC)
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("total_rampdown_required", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_required", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_power", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_power", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_required", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("total_rampup_required", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  val SCHEMA_BIDLADDERTOTAL = SchemaBuilder.struct().name(BID_LADDER_TOPIC)
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("rampdown_60", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_15_60", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_0_15", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_0_15", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_15_60", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_60_240", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_240_480", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_480", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  val SCHEMA_IMBALANCEPRICE = SchemaBuilder.struct().name(IMBALANCE_TOPIC)
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("upward_incident_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("downward_incident_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("upward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("downward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("incentive_component", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("take_from_system", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("feed_into_system", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("regulation_state", Schema.OPTIONAL_INT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  val SCHEMA_PRICELADDER = SchemaBuilder.struct().name(PRICE_LADDER_TOPIC)
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("neg_total", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_max", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_600", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_300", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_100", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_min", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_min", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_100", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_300", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_600", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_max", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_total", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  val SCHEMA_SETTLED_RRP = SchemaBuilder.struct().name(SETTLED_RRP_TOPIC)
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("downward_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("downward_power", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("downward_incident_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("upward_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("upward_power", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("upward_incident_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("volume", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("totals", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  val config: ConfigDef = new ConfigDef()
    .define(BALANCE_DELTA_TOPIC, Type.STRING, Importance.HIGH, BALANCE_DELTA_TOPIC_DOC)
    .define(IMBALANCE_TOPIC, Type.STRING, Importance.HIGH, IMBALANCE_TOPIC_DOC)
    .define(BID_LADDER_TOPIC, Type.STRING, Importance.HIGH, BID_LADDER_DOC)
    .define(BID_LADDER_TOTAL_TOPIC, Type.STRING, Importance.HIGH, BID_LADDER_TOTAL_DOC)
    .define(PRICE_LADDER_TOPIC, Type.STRING, Importance.HIGH, PRICE_LADDER_DOC)
    .define(SETTLED_RRP_TOPIC, Type.STRING, Importance.HIGH, SETTLED_RRP_DOC)
    .define(URL, Type.STRING, URL_DEFAULT, Importance.HIGH, URL_DOC)
    .define(REFRESH_RATE, Type.STRING, REFRESH_RATE_DEFAULT, Importance.LOW, REFRESH_RATE_DOC)
    .define(MAX_BACK_OFF, Type.STRING, MAX_BACK_OFF_DEFAULT, Importance.LOW, MAX_BACK_OFF_DOC)
    .define(TIMEZONE, Type.STRING, TIMEZONE_DEFAULT, Importance.HIGH, TIMEZONE_DOC)
}

