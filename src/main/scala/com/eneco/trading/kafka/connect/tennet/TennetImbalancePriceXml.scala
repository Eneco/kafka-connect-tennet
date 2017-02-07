package com.eneco.trading.kafka.connect.tennet

import java.text.SimpleDateFormat
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

object TennetImbalancePriceXml {
  private val offsetCache = mutable.Map[String, util.Map[String, Any]]()
}

case class TennetImbalancePriceXml(storageReader: OffsetStorageReader, url: String) extends StrictLogging {


  //TODO fix day break
  private val date = new SimpleDateFormat("yyyyMMdd").format(LocalDate.now().plusDays(-1))


  private  val offset = getConnectOffset(date)
  private val generatedAt = Instant.now.toEpochMilli
  private val bidladderTotalUrl = url.concat(s"/imbalanceprice/$date.xml")
  private val body=  Http(bidladderTotalUrl).asString.body
  private val hash = DigestUtils.sha256Hex(body)


  def fromBody(): Seq[ImbalancePriceRecord] = {
    val ladder = scala.xml.XML.loadString(body)

    (ladder \\ "Record").map(record =>
      ImbalancePriceRecord(
        (record \ "DATE").text.toString,
        (record \ "PTU").text.toInt,
        (record \ "PERIOD_FROM").text.toString,
        (record \ "PERIOD_UNTIL").text.toString,
        (record \ "UPWARD_INCIDENT_RESERVE").text.toDouble,
        (record \ "DOWNWARD_INCIDENT_RESERVE").text.toDouble,
        (record \ "UPWARD_DISPATCH").text.toDouble,
        (record \ "DOWNWARD_DISPATCH").text.toDouble,
        (record \ "INCENTIVE_COMPONENT").text.toDouble,
        (record \ "TAKE_FROM_SYSTEM").text.toDouble,
        (record \ "FEED_INTO_SYSTEM").text.toDouble,
        (record \ "REGULATION_STATE").text.toInt
      ))
  }


  def NodeSeqToDouble(value: NodeSeq) : Option[Double] = if (value.text.nonEmpty) Some(value.text.toDouble) else None

  def filter(): Seq[ImbalancePriceRecord] = fromBody().filter(isProcessed(_)).sortBy(_.PTU)

  def isProcessed(record: ImbalancePriceRecord) : Boolean = {
    hash.equals(offset.get.get("hash"))
  }

  def connectOffsetFromRecord(record: ImbalancePriceRecord): util.Map[String, Any] = {
    val offset = Map("sequence" -> record.PTU,
      "hash" -> hash
    ).asJava
    TennetImbalancePriceXml.offsetCache.put(date,offset)
    offset
  }

  def getConnectOffset(date: String): Option[util.Map[String, Any]] = TennetImbalancePriceXml.offsetCache.get(date).orElse(getOffsetFromStorage(date))

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
  //TODO?? replace partition with  "imbalanceprice"?
  def connectPartition(): util.Map[String, String] = Map("partition" -> date)
}

case class ImbalancePriceRecord(
                            Date: String,
                            PTU: Long,
                            PeriodFrom: String,
                            PeriodUntil: String,
                            UpwardIncidentReserve: Double,
                            DownwardIncidentReserve: Double,
                            UpwardDispatch: Double,
                            DownwardDispatch: Double,
                            IncentiveComponent: Double,
                            TakeFromSystem:Double,
                            FeedIntoSystem: Double,
                            RegulationState: Long
                          ) extends Record

