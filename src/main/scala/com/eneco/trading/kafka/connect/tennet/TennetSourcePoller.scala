package com.eneco.trading.kafka.connect.tennet

import java.time.Duration

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.util.{Failure, Success, Try}

class TennetSourcePoller(cfg: TennetSourceConfig, offsetStorageReader: OffsetStorageReader) extends StrictLogging {
  private val imbalanceTopic = cfg.getString(TennetSourceConfig.IMBALANCE_TOPIC)
  private val bidLadderTopic = cfg.getString(TennetSourceConfig.BID_LADDER_TOPIC)
  private val bidLaddertotalTopic = "TODO"
  private val settlementPriceTopic = cfg.getString(TennetSourceConfig.SETTLEMENT_PRICE_TOPIC)
  private val url = cfg.getString(TennetSourceConfig.URL)
  private val interval  = Duration.parse(cfg.getString(TennetSourceConfig.REFRESH_RATE))
  private val maxBackOff = Duration.parse(cfg.getString(TennetSourceConfig.MAX_BACK_OFF))
  var backoff = new ExponentialBackOff(interval, maxBackOff)

  def poll(): Seq[SourceRecord] = {
    Thread.sleep(interval.getSeconds * 1000)

    if (!backoff.passed) {
      return List[SourceRecord]()
    }

    //FIXME try catch block
    val records = Try(
      TennetSourceRecordProducer(offsetStorageReader).produce("imbalance",imbalanceTopic,url)) match {
      case Success(s) => s
      case Failure(f) =>
        backoff = backoff.nextFailure()
        logger.error(s"Error trying to retrieve data. ${f.getMessage}")
        logger.info(s"Backing off. Next poll will be around ${backoff.endTime}")
        List.empty[SourceRecord]
    }

    records :+ Try(TennetSourceRecordProducer(offsetStorageReader).produce("bidladder",bidLadderTopic,url))
    records :+ Try(TennetSourceRecordProducer(offsetStorageReader).produce("bidladdertotal",bidLaddertotalTopic,url))
    records :+ Try(TennetSourceRecordProducer(offsetStorageReader).produce("imbalanceprice",settlementPriceTopic,url))

    logger.info(s"Next poll will be around ${backoff.endTime}")
    records
  }
}