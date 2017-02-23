package com.eneco.trading.kafka.connect.tennet

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.immutable.Stream.Empty
import scala.xml.Node
import org.apache.kafka.connect.data.Schema

import scala.util.{Success, Try, Failure}


abstract class SourceRecordProducer(services: ServiceProvider, sourceType: SourceType) extends StrictLogging {

  def schema: Schema
  def mapRecord(record: Node, generatedAt: Long) : Object

  val epochMillis = EpochMillis(sourceType.timeZone)
  var offsetCache: Option[Map[String, String]] =  None

  def dateRange(forward: Int, backward: Int) = {
    List.range(-backward, forward + 1).map(n => LocalDate.now(services.clock).plusDays(n))
  }

  def produce : Seq[SourceRecord] = {
    val dates = List.range(- sourceType.backwardDays, sourceType.forwardDays + 1)
      .map(n => LocalDate.now(services.clock).plusDays(n))
      .map(DateTimeFormatter.ofPattern("yyyyMMdd").format(_))
    logger.trace(s"Querying dates: $dates")

    truncateOffsets(date => dates.contains(date))

    dates.flatMap(produceDate(_, dates))
  }

  def produceDate(date: String, dates: List[String]) : Seq[SourceRecord] = {
    val url = sourceType.baseUrl.concat(s"${sourceType.name}/$date.xml")
    val generatedAt = Instant.now(services.clock).toEpochMilli

    services.xmlReader.getXml(url) match {
      case Some(body) => {
        val hash = DigestUtils.sha256Hex(body)

        Try(scala.xml.XML.loadString(body) \\ "Record") match {
          case Success(records) if hash != getOffset(date) => {
            setOffset(date, hash)
            records.map(r => new SourceRecord(
              sourcePartition,
              getOffsets.asJava,
              sourceType.topic,
              schema,
              mapRecord(r, generatedAt)))
          }
          case Failure(e) => logger.warn("Failed to convert XML", e); Empty
          case _ => Empty
        }
      }
      case _ => Empty
    }
  }

  def sourcePartition = Map("source" -> sourceType.name).asJava

  def setOffset(key: String, offset: String) = {
    offsetCache = Some(getOffsets ++ Map(key -> offset))
    logger.trace(s"Set offset $key -> $offset")

  }

  def getOffset(key: String) = {
    val offset = getOffsets.getOrElse(key, "")
    logger.trace(s"Get offset $key -> $offset")
    offset
  }

  def truncateOffsets(filter: (String) => Boolean) = {
    offsetCache = Some(getOffsets.filterKeys(filter(_)))
    logger.trace(s"Truncated offsets ${getOffsets}")
  }

  def getOffsets : Map[String, String] = {
    offsetCache match {
      case Some(c) => c
      case _ => {
        offsetCache = getOffsetFromStorage
        offsetCache.get
      }
    }
  }

  private def getOffsetFromStorage(): Option[Map[String, String]] = {
    logger.info(s"Recovering offset for $sourcePartition")

    services.storageReader.offset(sourcePartition) match {
      case null =>
        logger.info(s"No offset found for $sourcePartition")
        Option(Map[String, String]())
      case o =>
        logger.info(s"Offset for $sourcePartition is : ${o.toString}")
        Option(o.asInstanceOf[util.Map[String, String]].asScala.toMap)
    }
  }
}


