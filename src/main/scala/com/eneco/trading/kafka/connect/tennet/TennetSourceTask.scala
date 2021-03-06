package com.eneco.trading.kafka.connect.tennet

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.mutable

class TennetSourceTask extends SourceTask with StrictLogging {
  private var poller: Option[TennetSourcePoller] = None
  private val progressCounter = new ProgressCounter(60000)

  override def stop(): Unit = {
    logger.info("Stopping Tennet SourceTask.")
  }

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Starting Tennet Source task.")
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/tennet-source-ascii.txt")).mkString)
    val sourceConfig = new TennetSourceConfig(props)
    poller = Some(new TennetSourcePoller(sourceConfig, context.offsetStorageReader()))
    logger.info("Task started.")
  }

  override def version(): String = "1.0.0"

  override def poll(): util.List[SourceRecord] = {
    val records = poller match {
      case Some(p) => p.poll()
      case _ => throw new RuntimeException("Polling before task started")
    }

    progressCounter.countRecords(records)
    records
  }

}

class ProgressCounter(periodMs: Int) extends StrictLogging{
  private val counter = mutable.Map.empty[String, Long]
  private var timestamp: Long = 0

  def countRecords(records: util.List[SourceRecord]): Unit = {
    records.foreach(r => counter.put(r.topic(), counter.getOrElse(r.topic(), 0L) + 1L))

    val newTimestamp = System.currentTimeMillis()
    if (counter.nonEmpty && scala.concurrent.duration.SECONDS.toSeconds(newTimestamp - timestamp) >= 60000) {
      counter.foreach({ case (k, v) => logger.info(s"Delivered $v records for $k.") })
      counter.empty
      timestamp = newTimestamp
    }
  }
}
