package com.eneco.trading.kafka.connect.tennet

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.xml.NodeSeq
import scalaj.http.Http

object TennetBidladderTotalXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetBidladderTotalXml(storageReader: OffsetStorageReader, url: String, isIntraday: Boolean) extends StrictLogging {


  //TODO fix day break
  private val date =if (isIntraday) { DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now) }
  else { DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now.plusDays(1))  }

  private  val offset = getConnectOffset(date)
  private val generatedAt = Instant.now.toEpochMilli
  private val bidladderTotalUrl = url.concat(s"laddersizetotal/$date.xml")
  private val body=  Http(bidladderTotalUrl).asString.body
  private val hash = DigestUtils.sha256Hex(body)


  def fromBody(): Seq[BidLadderTotalRecord] = {
    val ladder = scala.xml.XML.loadString(body)

    (ladder \\ "Record").map(record =>
      BidLadderTotalRecord(
        (record \ "DATE").text.toString,
        (record \ "PTU").text.toInt,
        (record \ "PERIOD_FROM").text.toString,
        (record \ "PERIOD_UNTIL").text.toString,
        NodeSeqToDouble(record \ "RAMPDOWN_60_").getOrElse(0),
        NodeSeqToDouble(record \ "RAMPDOWN_15_60").getOrElse(0),
        NodeSeqToDouble(record \ "RAMPDOWN_0_15").getOrElse(0),
        NodeSeqToDouble(record \ "RAMPUP_0_15").getOrElse(0),
        NodeSeqToDouble(record \ "RAMPUP_60_240").getOrElse(0),
        NodeSeqToDouble(record \ "RAMPUP_240_480").getOrElse(0),
        NodeSeqToDouble(record \ "RAMPUP_480_").getOrElse(0),
        generatedAt
      )
    )
  }

  def NodeSeqToDouble(value: NodeSeq) : Option[Double] = if (value.text.nonEmpty) Some(value.text.toDouble) else None

  def filter(): Seq[BidLadderTotalRecord] = fromBody().filter(isProcessed(_)).sortBy(_.PTU)

  def isProcessed(record: BidLadderTotalRecord) : Boolean = {
    !hash.equals(offset.get.get("hash"))
  }

  def connectOffsetFromRecord(record: BidLadderTotalRecord): util.Map[String, Any] = {
    val offset = Map("sequence" -> record.PTU,
      "hash" -> hash
    ).asJava
    TennetBidladderTotalXml.offsetCache.put(date,offset)
    offset
  }

  def getConnectOffset(date: String): Option[util.Map[String, Any]] = TennetBidladderTotalXml.offsetCache.get(date).orElse(getOffsetFromStorage(date))

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
  //TODO?? replace partition with  "bidladder"?
  def connectPartition(): util.Map[String, String] = Map("partition" -> date)
}

case class BidLadderTotalRecord(
                            Date: String,
                            PTU: Long,
                            PeriodFrom: String,
                            PeriodUntil: String,
                            Rampdown_60: Double,
                            Rampdown_0_15: Double,
                            Rampdown_15_60: Double,
                            Rampup_0_15: Double,
                            Rampup_60_240: Double,
                            Rampup_240_480: Double,
                            Rampup_480:Double,
                            GeneratedAt:Long
                          ) extends Record


