package com.eneco.trading.kafka.connect.tennet

import java.time.Duration

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.util.{Failure, Success, Try}

class TennetSourcePoller(cfg: TennetSourceConfig, offsetStorageReader: OffsetStorageReader) extends StrictLogging {
  val imbalanceTopic = cfg.getString(TennetSourceConfig.IMBALANCE_TOPIC)
  val bidLadderTopic = cfg.getString(TennetSourceConfig.BID_LADDER_TOPIC)
  val settlementPriceTopic = cfg.getString(TennetSourceConfig.SETTLEMENT_PRICE_TOPIC)
  val url = cfg.getString(TennetSourceConfig.URL)
  val interval  = Duration.parse(cfg.getString(TennetSourceConfig.REFRESH_RATE))
  val maxBackOff = Duration.parse(cfg.getString(TennetSourceConfig.MAX_BACK_OFF))
  var backoff = new ExponentialBackOff(interval, maxBackOff)

  def poll(): Seq[SourceRecord] = {
    Thread.sleep(interval.getSeconds * 1000)

    if (!backoff.passed) {
      return List[SourceRecord]()
    }

    val records = Try(TennetSourceRecordProducer(offsetStorageReader).produce(imbalanceTopic,url)) match {
      case Success(s) => s
      case Failure(f) => {
        backoff = backoff.nextFailure()
        logger.error(s"Error trying to retrieve data. ${f.getMessage}")
        logger.info(s"Backing off. Next poll will be around ${backoff.endTime}")
        List.empty[SourceRecord]
      }
    }
    logger.info(s"Next poll will be around ${backoff.endTime}")
    records
  }
}