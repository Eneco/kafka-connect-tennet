package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

class TennetSourcePoller(cfg: TennetSourceConfig, offsetStorageReader: OffsetStorageReader) extends StrictLogging {
  val topic = cfg.getString("topic")
  val url = cfg.getString("url")

  val interval  = cfg.getLong("interval")
  def poll(): Seq[SourceRecord] = {
    Thread.sleep(interval)
    logger.info("poll")
    TennetSourceRecordProducer(offsetStorageReader).produce(topic)
   }
}




