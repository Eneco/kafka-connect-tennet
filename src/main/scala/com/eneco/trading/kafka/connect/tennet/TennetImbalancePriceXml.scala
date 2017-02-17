package com.eneco.trading.kafka.connect.tennet

import java.time.format.DateTimeFormatter
import java.time.{ LocalDate}
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.xml.{Node}

object TennetImbalancePriceXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetImbalancePriceXml(storageReader: OffsetStorageReader, sourceType : SourceType)
  extends SourceRecordProducer with StrictLogging {

  private val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now.plusDays(-1))

  override val url = sourceType.baseUrl.concat(s"/${sourceType.name}/$date.xml")

  override def produce: Seq[SourceRecord] = {
    fromBody.map(r =>
      new SourceRecord(
        connectPartition,
        connectOffsetFromRecord(r),
        sourceType.topic,
        ImbalancePriceSourceRecord.schema,
        ImbalancePriceSourceRecord.struct(r)))
  }

  override def sourceName = SourceName.IMBALANCE_PRICE_NAME

  override def mapRecord(record: Node): TennetSourceRecord  = {
    ImbalancePriceTennetRecord(
      (record \ "DATE").text.toString,
      (record \ "PTU").text.toInt,
      (record \ "PERIOD_FROM").text.toString,
      (record \ "PERIOD_UNTIL").text.toString,
      TennetHelper.NodeSeqToDouble(record \ "UPWARD_INCIDENT_RESERVE").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_INCIDENT_RESERVE").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "UPWARD_DISPATCH").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "DOWNWARD_DISPATCH").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "INCENTIVE_COMPONENT").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "TAKE_FROM_SYSTEM").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "FEED_INTO_SYSTEM").getOrElse(0),
      (record \ "REGULATION_STATE").text.toInt,
      generatedAt,
      epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt)
    )
  }

  private val offset = getConnectOffset(date)

  private def fromBody(): Seq[ImbalancePriceTennetRecord] = {
    val ladder = scala.xml.XML.loadString(body)
    (ladder \\ "Record").map(record => mapRecord(record)).filter(isProcessed(_)).asInstanceOf[Seq[ImbalancePriceTennetRecord]].sortBy(_.PTU)
  }

  private def isProcessed(record: TennetSourceRecord): Boolean = {
    !hash.equals(offset.get.get("hash"))
  }

  private def connectOffsetFromRecord(record: ImbalancePriceTennetRecord): util.Map[String, Any] = {
    val offset = Map("sequence" -> record.PTU,
      "hash" -> hash
    ).asJava
    TennetImbalancePriceXml.offsetCache.put(date, offset)
    offset
  }

  private def getConnectOffset(date: String): Option[util.Map[String, Any]] = TennetImbalancePriceXml.offsetCache.get(date).orElse(getOffsetFromStorage(date))

  private def getOffsetFromStorage(name: String): Option[util.Map[String, Any]] = {
    logger.info(s"Recovering offset for $name")
    storageReader.offset(Map("partition" -> date).asJava) match {
      case null =>
        logger.info(s"No offset found for $name")
        Option(Map("sequence" -> 0l, "hash" -> "").asJava)
      case o =>
        logger.info(s"Offset for $name is : ${o.toString}")
        Option(o.asInstanceOf[util.Map[String, Any]])
    }
  }

  private def connectPartition(): util.Map[String, String] = Map("partition" -> date)
}

