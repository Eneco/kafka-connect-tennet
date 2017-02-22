package com.eneco.trading.kafka.connect.tennet

import java.time.Instant

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.immutable.Stream.Empty
import scala.xml.Node

case class TennetImbalanceXml(services: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(services, sourceType) with StrictLogging{

  val OFFSET_MAX_MILLIS = 60 * 60 * 1000;

  override def schema = ImbalanceSourceRecord.schema

  override def produce : Seq[SourceRecord] = {
    val url = sourceType.baseUrl.concat(s"${sourceType.name}/balans-delta.xml")

    services.xmlReader.getXml(url) match {
      case Some(body) => {
        val generatedAt = Instant.now(services.clock).toEpochMilli
        val records = (scala.xml.XML.loadString(body) \\ "RECORD")
          .map(mapRecord(_, generatedAt))
          .filter(r => getOffset(r.ValueTime.toString()) == "")

        if (!records.isEmpty) {
          val newestValueTime = records.maxBy(_.ValueTime).ValueTime
          truncateOffsets(k => k.toLong > newestValueTime - OFFSET_MAX_MILLIS)
        }

        records.map(r => {
          setOffset(r.ValueTime.toString, generatedAt.toString)
          new SourceRecord(
            sourcePartition,
            getOffsets.asJava,
            sourceType.topic,
            schema,
            ImbalanceSourceRecord.struct(r))
        })

      }
      case _ => Empty
    }
  }

  override def mapRecord(record: Node, generatedAt: Long): ImbalanceTennetRecord = {
    ImbalanceTennetRecord(
      (record \ "NUMBER").text.toInt,
      (record \ "SEQUENCE_NUMBER").text.toInt,
      (record \ "TIME").text,
      TennetHelper.NodeSeqToDouble(record \ "IGCCCONTRIBUTION_UP").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "IGCCCONTRIBUTION_DOWN").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "UPWARD_DISPATCH").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_DISPATCH").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RESERVE_UPWARD_DISPATCH").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RESERVE_DOWNWARD_DISPATCH").getOrElse(0),
      (record \ "INCIDENT_RESERVE_UP_INDICATOR").text.toString,
      (record \ "INCIDENT_RESERVE_DOWN_INDICATOR").text.toString,
      TennetHelper.NodeSeqToDouble(record \ "MIN_PRICE").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "MID_PRICE").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "MAX_PRICE").getOrElse(0),
      generatedAt,
      epochMillis.fromMinutes(generatedAt, (record \ "TIME").text.toString)
    )
  }
}