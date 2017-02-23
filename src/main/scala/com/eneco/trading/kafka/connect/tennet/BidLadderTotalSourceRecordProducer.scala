package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.xml.Node

case class BidLadderTotalSourceRecordProducer(readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = BidLadderTotalSourceRecord.schema

  override def mapRecord(record: Node, generatedAt: Long): Object  = {
    BidLadderTotalSourceRecord.struct(
      BidLadderTotalSourceRecord(
        (record \ "DATE").text.toString,
        (record \ "PTU").text.toInt,
        (record \ "PERIOD_FROM").text.toString,
        (record \ "PERIOD_UNTIL").text.toString,
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_60_").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_15_60").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_0_15").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_0_15").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_15_60").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_60_240").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_240_480").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_480_").getOrElse(0),
        generatedAt,
        epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt)
      )
    )
  }
}