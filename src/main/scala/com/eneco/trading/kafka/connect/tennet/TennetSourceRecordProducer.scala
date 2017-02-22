package com.eneco.trading.kafka.connect.tennet

import java.time.LocalDate

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

case class TennetSourceRecordProducer(offsetStorageReader: OffsetStorageReader) extends StrictLogging {

  def produce(sourceType: SourceType): Seq[SourceRecord] = {

    sourceType.name match {
      case SourceName.BALANCE_DELTA_NAME =>
        lazy val producer = TennetImbalanceXml(offsetStorageReader, sourceType)
        producer.produce

      case SourceName.BIDLADDER_NAME =>
        TennetHelper.createNextDaysList(2).flatMap(l => TennetBidladderXml(offsetStorageReader, sourceType, l).produce)

      case SourceName.BIDLADDER_TOTAL_NAME =>
        TennetHelper.createNextDaysList(2).flatMap(l => TennetBidladderTotalXml(offsetStorageReader, sourceType, l).produce)

      case SourceName.IMBALANCE_PRICE_NAME =>
        TennetHelper.createPrevDaysList(4).flatMap(l => TennetImbalancePriceXml(offsetStorageReader, sourceType, l).produce)

      case SourceName.PRICE_LADDER_NAME =>
        TennetHelper.createNextDaysList(2).flatMap(l => PriceLadderXml(offsetStorageReader, sourceType, l).produce)


      case _ =>
        logger.warn("Unknown type")
        List.empty[SourceRecord]
    }
  }
}
