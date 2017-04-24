package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.xml.Node

case class BidLadderSourceRecordProducer(readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = BidLadderSourceRecord.schema

  def mapRecord(record: Node, generatedAt: Long): Object = {
    BidLadderSourceRecord.struct(
      BidLadderSourceRecord(
        (record \ "DATE").text.toString,
        (record \ "PTU").text.toInt,
        (record \ "PERIOD_FROM").text.toString,
        (record \ "PERIOD_UNTIL").text.toString,
        TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPDOWN_REQUIRED"),
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_REQUIRED"),
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_RESERVE"),
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_POWER"),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_POWER"),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_RESERVE"),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_REQUIRED"),
        TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPUP_REQUIRED"),
        generatedAt,
        epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt)
      )
    )
  }
}