package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.xml.Node

case class ImbalancePriceSourceRecordProducer(readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = ImbalancePriceSourceRecord.schema;

  override def mapRecord(record: Node, generatedAt: Long): Object  = {
    /*
    ImbalancePriceSourceRecord.struct(
      ImbalancePriceSourceRecord(
        (record \ "DATE").text.toString,
        (record \ "PTU").text.toInt,
        (record \ "PERIOD_FROM").text.toString,
        (record \ "PERIOD_UNTIL").text.toString,
        TennetHelper.NodeSeqToDouble(record \ "UPWARD_INCIDENT_RESERVE"),
        TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_INCIDENT_RESERVE"),
        TennetHelper.NodeSeqToDouble(record \ "UPWARD_DISPATCH"),
        TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_DISPATCH"),
        TennetHelper.NodeSeqToDouble(record \ "INCENTIVE_COMPONENT"),
        TennetHelper.NodeSeqToDouble(record \ "TAKE_FROM_SYSTEM"),
        TennetHelper.NodeSeqToDouble(record \ "FEED_INTO_SYSTEM"),
        (record \ "REGULATION_STATE").text.toInt,
        generatedAt,
        epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt)
      )
    )
    */
    null
  }
}

