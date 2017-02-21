package com.eneco.trading.kafka.connect.tennet

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId}
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.xml.Node
import scalaj.http.Http

object TennetBidladderTotalXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetBidladderTotalXml(storageReader: OffsetStorageReader, sourceType : SourceType, localDate: LocalDate)
  extends SourceRecordProducer with StrictLogging {

  private val date: String = DateTimeFormatter.ofPattern("yyyyMMdd").format(localDate)

  val epochMillis = EpochMillis(sourceType.timeZone)

  override val url =  sourceType.baseUrl.concat(s"${sourceType.name}/$date.xml")

  override def produce : Seq[SourceRecord] = {
    fromBody.map(r =>
      new SourceRecord(
        connectPartition,
        connectOffsetFromRecord(r),
        sourceType.topic,
        BidLadderTotalSourceRecord.schema,
        BidLadderTotalSourceRecord.struct(r)))
  }

  override def sourceName: String = SourceName.BIDLADDER_TOTAL_NAME.toString

  override def mapRecord(record: Node): TennetSourceRecord  = {
    BidLadderTotalTennetRecord(
      (record \ "DATE").text.toString,
      (record \ "PTU").text.toInt,
      (record \ "PERIOD_FROM").text.toString,
      (record \ "PERIOD_UNTIL").text.toString,
      TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_60_").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_15_60").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_0_15").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPUP_0_15").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPUP_60_240").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPUP_240_480").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPUP_480_").getOrElse(0),
      generatedAt,
      epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt)
    )
  }

  private val offset = getConnectOffset(date)

  private def fromBody(): Seq[BidLadderTotalTennetRecord] = {
    val ladder = scala.xml.XML.loadString(body)
    (ladder \\ "Record").map(record =>
      mapRecord(record)).filter(isProcessed(_)).asInstanceOf[Seq[BidLadderTotalTennetRecord]].sortBy(_.PTU)
  }

  private def isProcessed(record: TennetSourceRecord): Boolean = {
    !hash.equals(offset.get.get("hash"))
  }

  private def connectOffsetFromRecord(record: BidLadderTotalTennetRecord): util.Map[String, Any] = {
    val offset = Map("sequence" -> record.PTU,
      "hash" -> hash
    ).asJava
    TennetBidladderTotalXml.offsetCache.put(date, offset)
    offset
  }

  private def getConnectOffset(date: String): Option[util.Map[String, Any]] = TennetBidladderTotalXml.offsetCache.get(date).orElse(getOffsetFromStorage(date))

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