package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

case class TennetSourceRecordProducer(offsetStorageReader: OffsetStorageReader) extends StrictLogging {

  def produce(sourceType: SourceType): Seq[SourceRecord] = {
    sourceType.name match {
      case SourceName.BALANCE_DELTA_NAME  =>
        lazy val producer = TennetImbalanceXml(offsetStorageReader, sourceType)
        producer.produce

      case SourceName.BIDLADDER_NAME  =>
        lazy val idProducer = TennetBidladderXml(offsetStorageReader, sourceType, isIntraday = true)
        lazy val daProducer = TennetBidladderXml(offsetStorageReader, sourceType, isIntraday = false)
        idProducer.produce ++ daProducer.produce

      case SourceName.BIDLADDER_TOTAL_NAME  =>
        lazy val idProducer = TennetBidladderTotalXml(offsetStorageReader, sourceType, isIntraday = true)
        lazy val daProducer = TennetBidladderTotalXml(offsetStorageReader, sourceType, isIntraday = false)
        idProducer.produce ++ daProducer.produce

      case SourceName.IMBALANCE_PRICE_NAME =>
        lazy val producer = TennetImbalancePriceXml(offsetStorageReader, sourceType)
        producer.produce
      case _ =>
        logger.warn("Unknown type")
        List.empty[SourceRecord]
    }
  }
}
