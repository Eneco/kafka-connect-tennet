package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.xml.Node

case class PriceLadderSourceRecordProducer(readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = PriceLadderSourceRecord.schema

  override def mapRecord(record: Node, generatedAt: Long): Object = {
    PriceLadderSourceRecord.struct(
      PriceLadderSourceRecord(
        (record \ "DATE").text.toString,
        (record \ "PTU").text.toInt,
        (record \ "PERIOD_FROM").text.toString,
        (record \ "PERIOD_UNTIL").text.toString,
        TennetHelper.NodeSeqToDouble(record \ "NEG_TOTAL"),
        TennetHelper.NodeSeqToDouble(record \ "NEG_MAX"),
        TennetHelper.NodeSeqToDouble(record \ "NEG_600"),
        TennetHelper.NodeSeqToDouble(record \ "NEG_300"),
        TennetHelper.NodeSeqToDouble(record \ "NEG_100"),
        TennetHelper.NodeSeqToDouble(record \ "NEG_MIN"),
        TennetHelper.NodeSeqToDouble(record \ "POS_MIN"),
        TennetHelper.NodeSeqToDouble(record \ "POS_100"),
        TennetHelper.NodeSeqToDouble(record \ "POS_300"),
        TennetHelper.NodeSeqToDouble(record \ "POS_600"),
        TennetHelper.NodeSeqToDouble(record \ "POS_MAX"),
        TennetHelper.NodeSeqToDouble(record \ "POS_TOTAL"),
        generatedAt,
        epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt)
      )
    )
  }

}