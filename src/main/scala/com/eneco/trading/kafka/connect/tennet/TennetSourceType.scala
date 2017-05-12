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
    val settledRRPTopic = cfg.getString(TennetSourceConfig.SETTLED_RRP_TOPIC)
    val url = cfg.getString(TennetSourceConfig.URL)
    val zoneId = ZoneId.of(cfg.getString(TennetSourceConfig.TIMEZONE))

    val balanceDelta = SourceType(SourceName.BALANCE_DELTA_NAME.toString, imbalanceBalanceTopic, url, zoneId, 0, 0)
    val bidLadder = SourceType(SourceName.BID_LADDER_NAME.toString, bidLadderTopic, url, zoneId, 1, 0)
    val bidLadderTotal = SourceType(SourceName.BID_LADDER_TOTAL_NAME.toString, bidLaddertotalTopic, url, zoneId, 1, 0)
    val imbalancePrice = SourceType(SourceName.IMBALANCE_PRICE_NAME.toString, settlementPriceTopic, url, zoneId, 0, 4)
    val priceLadder = SourceType(SourceName.PRICE_LADDER_NAME.toString, priceLadderTopic, url, zoneId, 1, 0)
    val settledRRP = SourceType(SourceName.SETTLED_RRP_NAME.toString, settledRRPTopic, url, zoneId, 0, 2)

    List[SourceType](balanceDelta, bidLadder, bidLadderTotal, imbalancePrice, priceLadder, settledRRP)
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
  val BID_LADDER_NAME = "laddersize15"
  val BID_LADDER_TOTAL_NAME = "laddersizetotal"
  val IMBALANCE_PRICE_NAME = "imbalanceprice"
  val PRICE_LADDER_NAME = "priceladder"
  val SETTLED_RRP_NAME = "pastrecordrrpdeployed"
}

