package com.eneco.trading.kafka.connect.tennet

import java.time.{Duration, ZoneId}
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging

object TennetSourceTypes {

  def createSources(cfg: TennetSourceConfig): List[SourceType] = {
    val imbalanceBalanceTopic = cfg.getString(TennetSourceConfig.BALANCE_DELTA_TOPIC)
    val bidLadderTopic = cfg.getString(TennetSourceConfig.BID_LADDER_TOPIC)
    val bidLaddertotalTopic = cfg.getString(TennetSourceConfig.BID_LADDER_TOTAL_TOPIC)
    val settlementPriceTopic = cfg.getString(TennetSourceConfig.IMBALANCE_TOPIC)
    val priceLadderTopic = cfg.getString(TennetSourceConfig.PRICE_LADDER_TOPIC)
    val url = cfg.getString(TennetSourceConfig.URL)
    val zoneId = ZoneId.of("Europe/Amsterdam")
    val dayahead = Duration.parse("P1D")
    val workingdays = Duration.parse("P-4D")


    val balanceDelta = SourceType(SourceName.BALANCE_DELTA_NAME.toString, imbalanceBalanceTopic, url, zoneId, 0, 0)
    val bidLadder = SourceType(SourceName.BIDLADDER_NAME.toString, bidLadderTopic, url, zoneId, 1, 0)
    val bidladderTotal = SourceType(SourceName.BIDLADDER_TOTAL_NAME.toString, bidLaddertotalTopic, url, zoneId, 1, 0)
    val imbalancePrice = SourceType(SourceName.IMBALANCE_PRICE_NAME.toString, settlementPriceTopic, url, zoneId, 0, 4)
    val priceLadder = SourceType(SourceName.PRICE_LADDER_NAME.toString, priceLadderTopic, url, zoneId, 1, 0)


    List[SourceType](balanceDelta, bidLadder, bidladderTotal, imbalancePrice,priceLadder)
  }
}

case class SourceType(name: String,
                      topic: String,
                      baseUrl: String,
                      timeZone: ZoneId,
                      forwardDays: Int,
                      backwardDays: Int)

object SourceName extends Enumeration {
  val BALANCE_DELTA_NAME = "balancedelta2017"
  val BIDLADDER_NAME = "laddersize15"
  val BIDLADDER_TOTAL_NAME = "laddersizetotal"
  val IMBALANCE_PRICE_NAME = "imbalanceprice"
  val PRICE_LADDER_NAME = "priceladder"
}

