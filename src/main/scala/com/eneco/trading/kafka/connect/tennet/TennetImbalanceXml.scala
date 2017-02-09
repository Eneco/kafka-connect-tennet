package com.eneco.trading.kafka.connect.tennet

import java.text.SimpleDateFormat
import java.time.Instant
import java.util
import java.util.Date

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.xml.NodeSeq
import scalaj.http.{Http, HttpResponse}

object TennetImbalanceXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetImbalanceXml(storageReader: OffsetStorageReader, url: String) extends StrictLogging {

  //TODO fix day break
  private val date = new SimpleDateFormat("dd-MM-yyyy").format(new Date)
  private  val offset = getConnectOffset(date)
  private val generatedAt = Instant.now.toEpochMilli
  val imbalanceUrl = url.concat("balancedelta2017/balans-delta.xml")
  val body =  (Http(imbalanceUrl).asString).body
  private val hash = DigestUtils.sha256Hex(body)


  def fromBody(): Seq[ImbalanceRecord] = {
    val imbalance = scala.xml.XML.loadString(body)
    (imbalance \\ "RECORD").map(record =>
      ImbalanceRecord(
        (record \ "NUMBER").text.toInt,
        (record \ "SEQUENCE_NUMBER").text.toInt,
        (record \ "TIME").text,
        NodeSeqToDouble(record \ "IGCCCONTRIBUTION_UP").getOrElse(0),
        NodeSeqToDouble(record \ "IGCCCONTRIBUTION_DOWN").getOrElse(0),
        NodeSeqToDouble(record \ "UPWARD_DISPATCH").getOrElse(0),
        NodeSeqToDouble(record \ "DOWNWARD_DISPATCH").getOrElse(0),
        NodeSeqToDouble(record \ "RESERVE_UPWARD_DISPATCH").getOrElse(0),
        NodeSeqToDouble(record \ "RESERVE_DOWNWARD_DISPATCH").getOrElse(0),
        (record \ "INCIDENT_RESERVE_UP_INDICATOR").text.toString,
        (record \ "INCIDENT_RESERVE_DOWN_INDICATOR").text.toString,
        NodeSeqToDouble(record \ "MIN_PRICE").getOrElse(0),
        NodeSeqToDouble(record \ "MID_PRICE").getOrElse(0),
        NodeSeqToDouble(record \ "MAX_PRICE").getOrElse(0),
        generatedAt
      )
    )
  }

  def NodeSeqToDouble(value: NodeSeq) : Option[Double] = {
    if (!value.text.isEmpty) Some(value.text.toDouble) else None
  }


  def filter(): Seq[ImbalanceRecord] = fromBody().filter(isProcessed(_)).sortBy(_.SequenceNumber)


  def isProcessed(record: ImbalanceRecord) : Boolean = {
    val lastSequence = offset.get.get("sequence")
    record.SequenceNumber > lastSequence.asInstanceOf[Long].longValue()
  }

  def connectOffsetFromRecord(record: ImbalanceRecord): util.Map[String, Any] = {
    val offset = Map("sequence" -> record.SequenceNumber,
      "hash" -> hash
    ).asJava
    TennetImbalanceXml.offsetCache.put(date,offset)
    offset
  }

  def getConnectOffset(date: String): Option[util.Map[String, Any]] = TennetImbalanceXml.offsetCache.get(date).orElse(getOffsetFromStorage(date))

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
  //TODO?? replace partition with  "imbalance"?
  def connectPartition(): util.Map[String, String] = Map("partition" -> date)
}



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
                            MaxPrice: Double,
                            GeneratedAt: Long
                          ) extends Record

