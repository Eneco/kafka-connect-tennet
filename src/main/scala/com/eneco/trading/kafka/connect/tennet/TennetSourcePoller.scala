package com.eneco.trading.kafka.connect.tennet

import java.time.Duration

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.util.{Failure, Success, Try}

class TennetSourcePoller(cfg: TennetSourceConfig, offsetStorageReader: OffsetStorageReader) extends StrictLogging {
  private val url = cfg.getString(TennetSourceConfig.URL)
  private val interval = Duration.parse(cfg.getString(TennetSourceConfig.REFRESH_RATE))
  private val maxBackOff = Duration.parse(cfg.getString(TennetSourceConfig.MAX_BACK_OFF))
  var backoff = new ExponentialBackOff(interval, maxBackOff)

  def poll(): Seq[SourceRecord] = {

    if (backoff.passed) {


      val records = Try(getRecords) match {
        case Success(recs) =>
          backoff = backoff.nextSuccess()
          logger.info(s"Next poll will be around ${backoff.endTime}")
          recs
        case Failure(f) =>
          backoff = backoff.nextFailure()
          logger.error(s"Error trying to retrieve data. ${f.getMessage}")
          logger.info(s"Backing off. Next poll will be around ${backoff.endTime}")
          List.empty[SourceRecord]
      }
      records
    }
    else {
      Thread.sleep(1000)
      List.empty[SourceRecord]
    }
  }

  def getRecords: Seq[SourceRecord] = {
    TennetSourceTypes.createSources(cfg).flatMap(sr =>
      TennetSourceRecordProducer(offsetStorageReader).produce(sr))
  }
}