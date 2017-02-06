package com.eneco.trading.kafka.connect.tennet

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.xml.NodeSeq

object TennetXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetXml(storageReader: OffsetStorageReader, body: String) extends StrictLogging {



  private val hash = DigestUtils.sha256Hex(body)

  //TODO fix day break
  private val date = new SimpleDateFormat("dd-MM-yyyy").format(new Date)
  private  val offset = getConnectOffset(date)

  def dummyFromBody(): Seq[ImbalanceRecord] = {
    //logger.info(body)
    val imbalance = scala.xml.XML.loadString(body)

    (imbalance \\ "RECORD").map(record =>
      ImbalanceRecord(
        (record \ "NUMBER").text.toInt,
        (record \ "SEQUENCE_NUMBER").text.toInt,
        (record \ "TIME").text,
        (record \ "IGCCCONTRIBUTION_UP").text.toDouble,
        (record \ "IGCCCONTRIBUTION_DOWN").text.toDouble,
        (record \ "UPWARD_DISPATCH").text.toDouble,
        (record \ "DOWNWARD_DISPATCH").text.toDouble,
        (record \ "RESERVE_UPWARD_DISPATCH").text.toDouble,
        (record \ "RESERVE_DOWNWARD_DISPATCH").text.toDouble,
        (record \ "INCIDENT_RESERVE_UP_INDICATOR").text.toString,
        (record \ "INCIDENT_RESERVE_DOWN_INDICATOR").text.toString,
        (record \ "MIN_PRICE").text.toDouble,
        (record \ "MID_PRICE").text.toDouble,
        (record \ "MAX_PRICE").text.toDouble
      )
    )
  }

  def fromBody(): Seq[ImbalanceRecord] = {
    logger.info(body)
    val imbalance = scala.xml.XML.loadString(body)
    (imbalance \\ "RECORD").map(record =>
      ImbalanceRecord(
        (record \ "NUMBER").text.toInt,
        (record \ "SEQUENCE_NUMBER").text.toInt,
        (record \ "TIME").text,
        (record \ "IGCCCONTRIBUTION_UP").text.toDouble,
        (record \ "IGCCCONTRIBUTION_DOWN").text.toDouble,
        (record \ "UPWARD_DISPATCH").text.toDouble,
        (record \ "DOWNWARD_DISPATCH").text.toDouble,
        (record \ "RESERVE_UPWARD_DISPATCH").text.toDouble,
        (record \ "RESERVE_DOWNWARD_DISPATCH").text.toDouble,
        (record \ "INCIDENT_RESERVE_UP_INDICATOR").text.toString,
        (record \ "INCIDENT_RESERVE_DOWN_INDICATOR").text.toString,
        NodeSeqToDouble(record \ "MIN_PRICE").getOrElse(0),
        NodeSeqToDouble(record \ "MID_PRICE").getOrElse(0),
        NodeSeqToDouble(record \ "MAX_PRICE").getOrElse(0)
      )
    )
  }

  def NodeSeqToDouble(value: NodeSeq) = {
    value.text match {
      case "" => None
      case s:String => Some(s.toDouble)
    }
  }


  def filter(): Seq[ImbalanceRecord] = {
    logger.info("filter")
    fromBody().filter(isProcessed(_)).sortBy(_.SequenceNumber)
  }

  def isProcessed(record: ImbalanceRecord) : Boolean = {
    val lastSequence = offset.get.get("sequence")
    record.SequenceNumber > lastSequence.asInstanceOf[Long].longValue()
  }

  def connectOffsetFromRecord(record: ImbalanceRecord): util.Map[String, Any] = {
    val offset = Map("sequence" -> record.SequenceNumber,
      "hash" -> hash
    ).asJava
    TennetXml.offsetCache.put(date,offset)
    offset
  }

  def getConnectOffset(date: String): Option[util.Map[String, Any]] = {
    logger.info("GetOffset")
    TennetXml.offsetCache.get(date).orElse(getOffsetFromStorage(date))
  }

  def getOffsetFromStorage(name: String): Option[util.Map[String, Any]] = {
    logger.info("GetOffsetFromStorage")
    storageReader.offset(Map("partition" -> date).asJava) match {
      case null =>
        logger.info("no offset found")
        Option(Map("sequence" -> 0l,
          "hash" -> "").asJava)
      case o =>
        logger.info(s"offset is : ${o.toString}")
        Option(o.asInstanceOf[util.Map[String, Any]])
    }
  }

  def connectPartition(): util.Map[String, String] = {
    Map("partition" -> date).asJava
  }

  case class TennetXmlOffset(fileName: String, sequence: Int, day: String, hash: String) {
    override def toString() = s"(filename:${fileName}, sequence: ${sequence}, day: ${day}, hash: ${hash})"
  }

}

abstract class Record

case class ImbalanceRecord(
                            Number: Long,
                            SequenceNumber: Long,
                            Time: String,
                            IgcccontributionUp: Double,
                            IgcccontributionDown: Double,
                            UpwardDispatch: Double,
                            DownwardDispatch: Double,
                            ReserveUpwardDispatch: Double,
                            ReserveDownwardDispatch: Double,
                            IncidentReserveUpIndicator : String,
                            IncidentReserveDownIndicator : String,
                            MinPrice: Double,
                            MidPrice: Double,
                            MaxPrice: Double
                          ) extends Record




