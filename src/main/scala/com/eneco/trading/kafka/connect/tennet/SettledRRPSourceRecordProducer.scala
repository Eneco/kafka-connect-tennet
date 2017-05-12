package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct

import scala.xml.Node

case class SettledRRPSourceRecordProducer (readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = TennetSourceConfig.SCHEMA_SETTLED_RRP

  def mapRecord(record: Node, generatedAt: Long): Struct = {

    new Struct(schema)
      .put("date", (record \ "DATE").text.toString)
      .put("ptu", (record \ "PTU").text.toLong)
      .put("period_from", (record \ "PERIOD_FROM").text.toString)
      .put("period_until", (record \ "PERIOD_UNTIL").text.toString)
      .put("downward_reserve", TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_RESERVE"))
      .put("downward_power", TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_POWER"))
      .put("downward_incident_reserve", TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_INCIDENT_RESERVE"))
      .put("upward_reserve", TennetHelper.NodeSeqToDouble(record \ "UPWARD_RESERVE"))
      .put("upward_power", TennetHelper.NodeSeqToDouble(record \ "UPWARD_POWER"))
      .put("upward_incident_reserve", TennetHelper.NodeSeqToDouble(record \ "UPWARD_INCIDENT_RESERVE"))
      .put("volume", TennetHelper.NodeSeqToDouble(record \ "VOLUME"))
      .put("totals", TennetHelper.NodeSeqToDouble(record \ "TOTALS"))
      .put("generated_at", generatedAt)
      .put("ptu_start", epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt))
  }
}
