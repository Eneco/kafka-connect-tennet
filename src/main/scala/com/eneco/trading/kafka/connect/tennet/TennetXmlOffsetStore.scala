package com.eneco.trading.kafka.connect.ftp.source

import java.time.Instant
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.collection.mutable

class TennetConnectData(storageReader: OffsetStorageReader) extends StrictLogging {

  private val cache = mutable.Map[String, TennetXmlOffset]()

  def get(filename: String): Option[TennetXmlOffset] = cache.get(filename).orElse({
      val stored = getFromStorage(filename)
      stored.foreach(set(filename,_))
      stored
    })

  def set(fileName: String, xmlOffset: TennetXmlOffset): Unit = {
    logger.info(s"TennetXmlOffsetStore set ${fileName}")
    cache.put(fileName, xmlOffset)
  }

  // cache couldn't provide us the info. this is a rather expensive operation (?)
  def getFromStorage(filename: String): Option[TennetXmlOffset] =
    storageReader.offset(Map("filename" -> filename).asJava) match {
      case null =>
        logger.warn(s"offset not found for ${filename}")
        None
      case o =>
        Some(connectOffsetToFileMetas(filename, o))
    }

  def fileMetasToConnectPartition(offset:TennetXmlOffset): util.Map[String, String] = {
    Map("filename" -> offset.fileName).asJava
  }

  def connectOffsetToFileMetas(filename:String, o:AnyRef): TennetXmlOffset = {
    val map= o.asInstanceOf[java.util.Map[String, AnyRef]]
    TennetXmlOffset(
      filename,
      map.get("hash").asInstanceOf[String],
      map.get("dailysequence").asInstanceOf[Int],
      Instant.ofEpochMilli(map.get("firstfetched").asInstanceOf[Long]),
      Instant.ofEpochMilli(map.get("lastmodified").asInstanceOf[Long]),
      Instant.ofEpochMilli(map.get("lastinspected").asInstanceOf[Long])
    )
  }

  def xmlOffsetToConnectOffset(xmlOffset: TennetXmlOffset): util.Map[String, Any] = {
    Map("dailysequence" -> xmlOffset.maxDailySequence,
      "hash" -> xmlOffset.lastTime,
      "firstfetched" -> xmlOffset.firstFetched.toEpochMilli,
      "lastmodified" -> xmlOffset.lastModified.toEpochMilli,
      "lastinspected" -> xmlOffset.lastInspected.toEpochMilli
    ).asJava
  }
}


// used to administer the files
// this is persistent data, stored into the connect offsets
case class TennetXmlOffset(fileName:String, lastTime:String, maxDailySequence:Int, firstFetched:Instant, lastModified:Instant, lastInspected:Instant) {
  def modifiedNow() = TennetXmlOffset(fileName, lastTime, maxDailySequence, firstFetched, Instant.now, lastInspected)
  def inspectedNow() = TennetXmlOffset(fileName, lastTime, maxDailySequence,firstFetched, lastModified, Instant.now)
  override def toString() = s"(filename:${fileName}, hash: ${lastTime}, dailySequence: ${maxDailySequence},firstFetched: ${firstFetched}, lastModified: ${lastModified}, lastInspected: ${lastInspected}"
}
