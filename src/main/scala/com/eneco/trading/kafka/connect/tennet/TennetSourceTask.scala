package com.eneco.trading.kafka.connect.tennet

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._

class TennetSourceTask extends SourceTask with StrictLogging {
  var poller: Option[TennetSourcePoller] = None

  override def stop(): Unit = {
    logger.info("Stop")
  }


  override def start(props: util.Map[String, String]): Unit = {
    logger.info("start")
    val sourceConfig = new TennetSourceConfig(props)
    poller = Some(new TennetSourcePoller(sourceConfig, context.offsetStorageReader()))
    logger.info("start")
  }

  override def version(): String = "1.0.0"

  override def poll(): util.List[SourceRecord] = poller match{
    case Some(poller) =>poller.poll.asJava
    case None => throw new ConnectException("SampleSource task is not initialized but it is polled")
  }
}
