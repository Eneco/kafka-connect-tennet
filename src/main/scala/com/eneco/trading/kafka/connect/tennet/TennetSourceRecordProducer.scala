package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

case class TennetSourceRecordProducer(offsetStorageReader: OffsetStorageReader) extends StrictLogging {

  def produce(sourceType: SourceType): Seq[SourceRecord] = {
    sourceType.name match {
      case SourceName.BALANCE_DELTA_NAME  =>
        TennetImbalanceXml(offsetStorageReader, sourceType).produce

      case SourceName.BIDLADDER_NAME  =>
        val id = (TennetBidladderXml(offsetStorageReader, sourceType, isIntraday = true)).produce
        val da =   (TennetBidladderXml(offsetStorageReader, sourceType, isIntraday = false)).produce
        id++da

      case SourceName.BIDLADDER_TOTAL_NAME  =>
        val id = (TennetBidladderTotalXml(offsetStorageReader, sourceType, isIntraday = true)).produce
        val da =   (TennetBidladderTotalXml(offsetStorageReader, sourceType, isIntraday = false)).produce
        id++da

      case SourceName.IMBALANCE_PRICE_NAME =>
        TennetImbalancePriceXml(offsetStorageReader, sourceType).produce
      case _ =>
        logger.warn("Unknown type")
        List.empty[SourceRecord]
    }
  }
}
