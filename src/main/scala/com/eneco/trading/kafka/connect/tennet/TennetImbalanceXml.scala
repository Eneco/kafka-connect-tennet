package com.eneco.trading.kafka.connect.tennet

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.xml.{Node}

object TennetImbalanceXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetImbalanceXml(storageReader: OffsetStorageReader,sourceType: SourceType)
  extends SourceRecordProducer with StrictLogging{

  override val url = sourceType.baseUrl.concat(s"${sourceType.name}/balans-delta.xml")

  val epochMillis = EpochMillis(sourceType.timeZone)

  override def produce: Seq[SourceRecord] = {
    fromBody.map(r =>
      new SourceRecord(
        connectPartition,
        connectOffsetFromRecord(r),
        sourceType.topic,
        ImbalanceSourceRecord.schema,
        ImbalanceSourceRecord.struct(r)))
  }

  override def sourceName: String = SourceName.BALANCE_DELTA_NAME.toString

  override def mapRecord(record: Node): ImbalanceTennetRecord = {
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

  private val date = new SimpleDateFormat("dd-MM-yyyy").format(new Date)

  private val offset = getConnectOffset(date)

  private def fromBody(): Seq[ImbalanceTennetRecord] = {
    val imbalance = scala.xml.XML.loadString(body)
    (imbalance \\ "RECORD")
      .map(record => mapRecord(record))
      .filter(isProcessed(_))
      .asInstanceOf[Seq[ImbalanceTennetRecord]]
      .sortBy(_.SequenceNumber)
  }

  private def isProcessed(record: ImbalanceTennetRecord): Boolean = {
    val lastSequence = offset.get.get("sequence")
    record.SequenceNumber > lastSequence.asInstanceOf[Long].longValue()
  }

  private def connectOffsetFromRecord(record: ImbalanceTennetRecord): util.Map[String, Any] = {
    val offset = Map("sequence" -> record.SequenceNumber,
      "hash" -> hash
    ).asJava
    TennetImbalanceXml.offsetCache.put(date, offset)
    offset
  }

  private def getConnectOffset(date: String): Option[util.Map[String, Any]] = TennetImbalanceXml.offsetCache.get(date).orElse(getOffsetFromStorage(date))

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