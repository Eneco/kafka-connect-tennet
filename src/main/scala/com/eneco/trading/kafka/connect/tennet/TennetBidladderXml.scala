package com.eneco.trading.kafka.connect.tennet

import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.xml.{Node, NodeSeq}
import scalaj.http.Http

object TennetBidladderXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetBidladderXml(storageReader: OffsetStorageReader, url: String, isIntraday: Boolean) extends StrictLogging {


  //TODO fix day break
  private val date =if (isIntraday) { DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now) }
      else { DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now.plusDays(1))  }


  private val offset = getConnectOffset(date)
  private val generatedAt = Instant.now.toEpochMilli
  private val bidladderUrl = url.concat(s"laddersize15/$date.xml")
  private val body=  Http(bidladderUrl).asString.body
  private val hash = DigestUtils.sha256Hex(body)


  def fromBody(): Seq[BidLadderRecord] = {
    val ladder = scala.xml.XML.loadString(body)

    (ladder \\ "Record").map(record =>
      mapRecord(record)).filter(isProcessed(_)).asInstanceOf[Seq[BidLadderRecord] ].sortBy(_.PTU)
  }

  def mapRecord(record : Node) : Record  = {
    BidLadderRecord(
      (record \ "DATE").text.toString,
      (record \ "PTU").text.toInt,
      (record \ "PERIOD_FROM").text.toString,
      (record \ "PERIOD_UNTIL").text.toString,
      TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPDOWN_REQUIRED").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_REQUIRED").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_RESERVE").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_POWER").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPUP_POWER").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPUP_RESERVE").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "RAMPUP_REQUIRED").getOrElse(0),
      TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPUP_REQUIRED").getOrElse(0),
      generatedAt,
      epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt)
    )
  }

  override def isProcessed(record: Record) : Boolean = {
    !hash.equals(offset.get.get("hash"))
  }

  def connectOffsetFromRecord(record: BidLadderRecord): util.Map[String, Any] = {
    val offset = Map("sequence" -> record.PTU,
      "hash" -> hash
    ).asJava
    TennetBidladderXml.offsetCache.put(date,offset)
    offset
  }

  def getConnectOffset(date: String): Option[util.Map[String, Any]] = TennetBidladderXml.offsetCache.get(date).orElse(getOffsetFromStorage(date))

  def getOffsetFromStorage(name: String): Option[util.Map[String, Any]] = {
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
  //TODO?? replace partition with  "bidlladder"?
  def connectPartition(): util.Map[String, String] = Map("partition" -> date)
}

case class BidLadderRecord(
                            Date: String,
                            PTU: Long,
                            PeriodFrom: String,
                            PeriodUntil: String,
                            TotalRampDownRequired: Double,
                            RampDownRequired: Double,
                            RampDownReserve: Double,
                            RampDownPower: Double,
                            RampUpPower: Double,
                            RampUpReserve: Double,
                            RampUpRequired:Double,
                            TotalRampUpRequired: Double,
                            GeneratedAt:Long
                          ) extends Record
