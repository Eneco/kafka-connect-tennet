package com.eneco.trading.kafka.connect.tennet

import java.time.{Clock, Duration}

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.util.{Failure, Success, Try}

class TennetSourcePoller(cfg: TennetSourceConfig, offsetStorageReader: OffsetStorageReader) extends StrictLogging {

  private val interval = Duration.parse(cfg.getString(TennetSourceConfig.REFRESH_RATE))
  private val maxBackOff = Duration.parse(cfg.getString(TennetSourceConfig.MAX_BACK_OFF))
  var backoff = new ExponentialBackOff(interval, maxBackOff)
  val sources = TennetSourceTypes.createSources(cfg).map(newProducer(_))

  object serviceProvider extends ServiceProvider {
    val storageReader: OffsetStorageReader = offsetStorageReader
    val xmlReader: XmlReader = HttpXmlReader
    val clock = Clock.systemUTC
  }

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

  def getRecords: Seq[SourceRecord] = sources.flatMap(sr => sr.produce)

  def newProducer(sourceType: SourceType) : SourceRecordProducer = {
    sourceType.name match {
      case SourceName.BALANCE_DELTA_NAME => TennetImbalanceXml(serviceProvider, sourceType)
      case SourceName.BIDLADDER_NAME => TennetBidladderXml(serviceProvider, sourceType)
      case SourceName.BIDLADDER_TOTAL_NAME => TennetBidladderTotalXml(serviceProvider, sourceType)
      case SourceName.IMBALANCE_PRICE_NAME => TennetImbalancePriceXml(serviceProvider, sourceType)
      case SourceName.PRICE_LADDER_NAME => PriceLadderXml(serviceProvider, sourceType)
      case _ => throw new RuntimeException(s"Unkown sourceType: $sourceType")
    }
  }
}