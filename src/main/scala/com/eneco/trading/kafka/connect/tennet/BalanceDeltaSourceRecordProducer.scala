package com.eneco.trading.kafka.connect.tennet

import java.time.Instant

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.immutable.Stream.Empty
import scala.util.{Failure, Success, Try}
import scala.xml.Node

case class BalanceDeltaSourceRecordProducer(services: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(services, sourceType) with StrictLogging{

  val OFFSET_MAX_MILLIS = 60 * 60 * 1000;

  override def schema = TennetSourceConfig.SCHEMA_IMBALANCE

  override def produce : Seq[SourceRecord] = {
    val url = sourceType.baseUrl.concat(s"${sourceType.name}/balans-delta.xml")

    services.xmlReader.getXml(url) match {
      case Some(body) => {
        val generatedAt = Instant.now(services.clock).toEpochMilli
        val records = Try(scala.xml.XML.loadString(body) \\ "RECORD") match {
          case Success(records) => records.map(mapRecord(_, generatedAt))
            .filter(r => getOffset(r.get("value_time").toString()) == "")
          case Failure(e) => logger.warn("Failed to load XML", e); Empty
        }


        if (!records.isEmpty) {
          val newestValueTime = records.map(_.getInt64("value_time")).max
          truncateOffsets(k => k.toLong > newestValueTime - OFFSET_MAX_MILLIS)
        }

        records.map(r => {
          setOffset(r.get("value_time").toString, generatedAt.toString)
          new SourceRecord(
            sourcePartition,
            getOffsets.asJava,
            sourceType.topic,
            schema,
            r)
        })

      }
      case _ => Empty
    }
  }

  override def mapRecord(record: Node, generatedAt: Long): Struct = {
    new Struct(schema)
      .put("number", (record \ "NUMBER").text.toLong)
      .put("sequence_number", (record \ "SEQUENCE_NUMBER").text.toLong)
      .put("time", (record \ "TIME").text)
      .put("igcccontribution_up", TennetHelper.NodeSeqToDouble(record \ "IGCCCONTRIBUTION_UP"))
      .put("igcccontribution_down", TennetHelper.NodeSeqToDouble(record \ "IGCCCONTRIBUTION_DOWN"))
      .put("upward_dispatch", TennetHelper.NodeSeqToDouble(record \ "UPWARD_DISPATCH"))
      .put("downward_dispatch", TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_DISPATCH"))
      .put("reserve_upward_dispatch", TennetHelper.NodeSeqToDouble(record \ "RESERVE_UPWARD_DISPATCH"))
      .put("reserve_downward_dispatch", TennetHelper.NodeSeqToDouble(record \ "RESERVE_DOWNWARD_DISPATCH"))
      .put("incident_reserve_up_indicator", TennetHelper.NodeSeqToDouble(record \ "INCIDENT_RESERVE_UP_INDICATOR"))
      .put("incident_reserve_down_indicator", TennetHelper.NodeSeqToDouble(record \ "INCIDENT_RESERVE_DOWN_INDICATOR"))
      .put("min_price", TennetHelper.NodeSeqToDouble(record \ "MIN_PRICE"))
      .put("mid_price", TennetHelper.NodeSeqToDouble(record \ "MID_PRICE"))
      .put("max_price", TennetHelper.NodeSeqToDouble(record \ "MAX_PRICE"))
      .put("generated_at", generatedAt)
      .put("value_time", epochMillis.fromMinutes(generatedAt, (record \ "TIME").text.toString))
  }
}