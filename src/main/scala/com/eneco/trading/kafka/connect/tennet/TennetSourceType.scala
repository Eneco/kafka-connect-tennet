package com.eneco.trading.kafka.connect.tennet

import java.time.{Duration, ZoneId}

object TennetSourceTypes {

  def createSources(cfg: TennetSourceConfig): List[SourceType] = {
    val imbalanceBalanceTopic = cfg.getString(TennetSourceConfig.BALANCE_DELTA_TOPIC)
    val bidLadderTopic = cfg.getString(TennetSourceConfig.BID_LADDER_TOPIC)
    val bidLaddertotalTopic = cfg.getString(TennetSourceConfig.BID_LADDER_TOTAL_TOPIC)
    val settlementPriceTopic = cfg.getString(TennetSourceConfig.IMBALANCE_TOPIC)
    val url = cfg.getString(TennetSourceConfig.URL)
    val zoneId = ZoneId.of("Europe/Amsterdam")
    val dayahead = Duration.parse("P1D")
    val workingdays = Duration.parse("P-4D")


    val balanceDelta = SourceType(SourceName.BALANCE_DELTA_NAME.toString, imbalanceBalanceTopic, url, zoneId)
    val bidLadder = SourceType(SourceName.BIDLADDER_NAME.toString, bidLadderTopic, url, zoneId)
    val bidladderTotal = SourceType(SourceName.BIDLADDER_TOTAL_NAME.toString, bidLaddertotalTopic, url, zoneId)
    val imbalancePrice = SourceType(SourceName.IMBALANCE_PRICE_NAME.toString, settlementPriceTopic, url, zoneId)

    List[SourceType](balanceDelta, bidLadder, bidladderTotal, imbalancePrice)
  }
}

case class SourceType(name: String, topic: String, baseUrl: String, timeZone: ZoneId) {
}

object SourceName extends Enumeration {
  val BALANCE_DELTA_NAME = "balancedelta2017"
  val BIDLADDER_NAME = "laddersize15"
  val BIDLADDER_TOTAL_NAME = "laddersizetotal"
  val IMBALANCE_PRICE_NAME = "imbalanceprice"
}

