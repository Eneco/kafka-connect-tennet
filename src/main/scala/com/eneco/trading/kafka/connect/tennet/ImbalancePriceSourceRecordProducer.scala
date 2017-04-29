package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct

import scala.xml.Node

case class ImbalancePriceSourceRecordProducer(readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = TennetSourceConfig.SCHEMA_IMBALANCEPRICE

  override def mapRecord(record: Node, generatedAt: Long): Struct  = {
    new Struct(schema)
      .put("date", (record \ "DATE").text.toString)
      .put("ptu", (record \ "PTU").text.toLong)
      .put("period_from", (record \ "PERIOD_FROM").text.toString)
      .put("period_until", (record \ "PERIOD_UNTIL").text.toString)
      .put("upward_incident_reserve", TennetHelper.NodeSeqToDouble(record \ "UPWARD_INCIDENT_RESERVE"))
      .put("downward_incident_reserve", TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_INCIDENT_RESERVE"))
      .put("upward_dispatch", TennetHelper.NodeSeqToDouble(record \ "UPWARD_DISPATCH"))
      .put("downward_dispatch", TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_DISPATCH"))
      .put("incentive_component", TennetHelper.NodeSeqToDouble(record \ "INCENTIVE_COMPONENT"))
      .put("take_from_system", TennetHelper.NodeSeqToDouble(record \ "TAKE_FROM_SYSTEM"))
      .put("feed_into_system", TennetHelper.NodeSeqToDouble(record \ "FEED_INTO_SYSTEM"))
      .put("regulation_state", (record \ "REGULATION_STATE").text.toLong)
      .put("generated_at", generatedAt)
      .put("ptu_start", epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt))
  }
}

