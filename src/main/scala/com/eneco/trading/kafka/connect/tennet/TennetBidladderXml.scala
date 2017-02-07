package com.eneco.trading.kafka.connect.tennet

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.xml.NodeSeq

object TennetBidladderXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetBidladderXml(storageReader: OffsetStorageReader, body: String) extends StrictLogging {

  private val hash = DigestUtils.sha256Hex(body)

  //TODO fix day break
  private val date = new SimpleDateFormat("dd-MM-yyyy").format(new Date)
  private  val offset = getConnectOffset(date)
  private val generatedAt = Instant.now.toEpochMilli

  def fromBody(): Seq[BidLadderRecord] = {
    logger.info(body)
    val ladder = scala.xml.XML.loadString(body)

    (ladder \\ "Record").map(record =>
      BidLadderRecord(
        (record \ "DATE").text.toString,
        (record \ "PTU").text.toInt,
        (record \ "PERIOD_FROM").text.toString,
        (record \ "PERIOD_UNTIL").text.toString,
        (record \ "TOTAL_RAMPDOWN_REQUIRED").text.toDouble,
        (record \ "RAMPDOWN_REQUIRED").text.toDouble,
        (record \ "RAMPDOWN_RESERVE").text.toDouble,
        (record \ "RAMPDOWN_POWER").text.toDouble,
        (record \ "RAMPUP_POWER").text.toDouble,
        (record \ "RAMPUP_RESERVE").text.toDouble,
        (record \ "RAMPUP_REQUIRED").text.toDouble,
        (record \ "TOTAL_RAMPUP_REQUIRED").text.toDouble,
        generatedAt
      ))
  }

  def NodeSeqToDouble(value: NodeSeq) : Option[Double] = if (value.text.nonEmpty) Some(value.text.toDouble) else None


  def filter(): Seq[BidLadderRecord] = fromBody().filter(isProcessed(_)).sortBy(_.PTU)


  def isProcessed(record: BidLadderRecord) : Boolean = {
    val formatter = DateTimeFormatter.ISO_DATE
    val date = LocalDate.parse(record.Date,formatter)
    hash.equals(offset.get.get("hash"))
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
