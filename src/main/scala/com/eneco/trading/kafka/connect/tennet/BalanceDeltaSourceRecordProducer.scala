package com.eneco.trading.kafka.connect.tennet

import java.time.Instant

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.immutable.Stream.Empty
import scala.util.{Try, Success, Failure}
import scala.xml.Node

case class BalanceDeltaSourceRecordProducer(services: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(services, sourceType) with StrictLogging{

  val OFFSET_MAX_MILLIS = 60 * 60 * 1000;

  override def schema = BalanceDeltaSourceRecord.schema

  override def produce : Seq[SourceRecord] = {
    val url = sourceType.baseUrl.concat(s"${sourceType.name}/balans-delta.xml")

    services.xmlReader.getXml(url) match {
      case Some(body) => {
        val generatedAt = Instant.now(services.clock).toEpochMilli
        val records = Try(scala.xml.XML.loadString(body) \\ "RECORD") match {
          case Success(records) => records.map(mapRecord(_, generatedAt))
            .filter(r => getOffset(r.ValueTime.toString()) == "")
          case Failure(e) => logger.warn("Failed to load XML", e); Empty
        }


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
            BalanceDeltaSourceRecord.struct(r))
        })

      }
      case _ => Empty
    }
  }

  override def mapRecord(record: Node, generatedAt: Long): BalanceDeltaSourceRecord = {
    BalanceDeltaSourceRecord(
      (record \ "NUMBER").text.toInt,
      (record \ "SEQUENCE_NUMBER").text.toInt,
      (record \ "TIME").text,
      TennetHelper.NodeSeqToDouble(record \ "IGCCCONTRIBUTION_UP"),
      TennetHelper.NodeSeqToDouble(record \ "IGCCCONTRIBUTION_DOWN"),
      TennetHelper.NodeSeqToDouble(record \ "UPWARD_DISPATCH"),
      TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_DISPATCH"),
      TennetHelper.NodeSeqToDouble(record \ "RESERVE_UPWARD_DISPATCH"),
      TennetHelper.NodeSeqToDouble(record \ "RESERVE_DOWNWARD_DISPATCH"),
      TennetHelper.NodeSeqToDouble(record \ "INCIDENT_RESERVE_UP_INDICATOR"),
      TennetHelper.NodeSeqToDouble(record \ "INCIDENT_RESERVE_DOWN_INDICATOR"),
      TennetHelper.NodeSeqToDouble(record \ "MIN_PRICE"),
      TennetHelper.NodeSeqToDouble(record \ "MID_PRICE"),
      TennetHelper.NodeSeqToDouble(record \ "MAX_PRICE"),
      generatedAt,
      epochMillis.fromMinutes(generatedAt, (record \ "TIME").text.toString)
    )
  }
}