package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct

import scala.xml.Node

case class SettledRRPSourceRecordProducer (readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = TennetSourceConfig.SCHEMA_BIDLADDER

  def mapRecord(record: Node, generatedAt: Long): Struct = {
    new Struct(schema)
      .put("date", (record \ "DATE").text.toString)
      .put("ptu", (record \ "PTU").text.toLong)
      .put("period_from", (record \ "PERIOD_FROM").text.toString)
      .put("period_until", (record \ "PERIOD_UNTIL").text.toString)
      .put("total_rampdown_required", TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPDOWN_REQUIRED"))
      .put("rampdown_required", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_REQUIRED"))
      .put("rampdown_reserve", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_RESERVE"))
      .put("rampdown_power", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_POWER"))
      .put("rampup_power", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_POWER"))
      .put("rampup_reserve", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_RESERVE"))
      .put("rampup_required", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_REQUIRED"))
      .put("total_rampup_required", TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPUP_REQUIRED"))
      .put("generated_at", generatedAt)
      .put("ptu_start", epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt))
  }
}

object Play extends App(){
  println("Hello world")

  val xmlstr =
    """
      |<Record>
      |    <DATE>2017-05-06T00:00:00</DATE>
      |    <PTU>1</PTU>
      |    <PERIOD_FROM>00:00</PERIOD_FROM>
      |    <PERIOD_UNTIL>00:15</PERIOD_UNTIL>
      |    <DOWNWARD_RESERVE/>
      |    <DOWNWARD_POWER>-15366</DOWNWARD_POWER>
      |    <UPWARD_POWER>849</UPWARD_POWER>
      |    <UPWARD_RESERVE/>
      |    <UPWARD_INCIDENT_RESERVE/>
      |    <VOLUME>16215</VOLUME>
      |    <TOTALS>-14517</TOTALS>
      | </Record>
    """.stripMargin

  val record = xml.XML.loadString(xmlstr)
  println((record \ "DATE").text.toString)

  /*
  val schema = null

  def mapRecord(record: Node, generatedAt: Long): Struct = {
    new Struct(schema)
      .put("date", (record \ "DATE").text.toString)
      .put("ptu", (record \ "PTU").text.toLong)
      .put("period_from", (record \ "PERIOD_FROM").text.toString)
      .put("period_until", (record \ "PERIOD_UNTIL").text.toString)
      .put("total_rampdown_required", TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPDOWN_REQUIRED"))
      .put("rampdown_required", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_REQUIRED"))
      .put("rampdown_reserve", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_RESERVE"))
      .put("rampdown_power", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_POWER"))
      .put("rampup_power", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_POWER"))
      .put("rampup_reserve", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_RESERVE"))
      .put("rampup_required", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_REQUIRED"))
      .put("total_rampup_required", TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPUP_REQUIRED"))
      .put("generated_at", generatedAt)
      .put("ptu_start", epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt))
  }
  */

}
