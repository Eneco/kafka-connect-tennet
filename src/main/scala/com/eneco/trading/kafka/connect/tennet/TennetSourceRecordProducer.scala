package com.eneco.trading.kafka.connect.tennet


import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scalaj.http.{Http, HttpResponse}



case class TennetSourceRecordProducer(offsetStorageReader: OffsetStorageReader) extends StrictLogging {

  var partition = "imbalance"

  def produce(topic: String,url :String): Seq[SourceRecord] = {
    logger.info("onProduce")

    val response: HttpResponse[String] = Http(url).asString
    val data = TennetXml(offsetStorageReader, response.body)
    data.filter().map(r =>
    new SourceRecord(
        data.connectPartition, //source partitions?
        data.connectOffsetFromRecord(r) ,
        topic,
        ImbalanceSourceRecord.schema,
        ImbalanceSourceRecord.struct(r))
    )
  }
  object ImbalanceSourceRecord {
    def struct(io: ImbalanceRecord) =
      new Struct(schema)
        .put("number", io.Number)
        .put("sequence_number", io.SequenceNumber)
        .put("time", io.Time)
        .put("igcccontribution_up", io.IgcccontributionUp)
        .put("igcccontribution_down", io.IgcccontributionDown)
        .put("upward_dispatch", io.UpwardDispatch)
        .put("downward_dispatch", io.DownwardDispatch)
        .put("reserve_upward_dispatch", io.ReserveUpwardDispatch)
        .put("reserve_downward_dispatch", io.ReserveDownwardDispatch)
        .put("incident_reserve_up_indicator", io.IncidentReserveUpIndicator)
        .put("incident_reserve_down_indicator", io.IncidentReserveUpIndicator)
        .put("reserve_upward_dispatch", io.ReserveUpwardDispatch)
        .put("reserve_downward_dispatch", io.ReserveDownwardDispatch)
        .put("min_price", io.MinPrice)
        .put("mid_price", io.MidPrice)
        .put("max_price", io.MaxPrice)


    val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet")
      .field("number", Schema.INT64_SCHEMA)
      .field("sequence_number", Schema.INT64_SCHEMA)
      .field("time", Schema.STRING_SCHEMA)
      .field("igcccontribution_up", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("igcccontribution_down", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("upward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("downward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("reserve_upward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("reserve_downward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("incident_reserve_up_indicator", Schema.OPTIONAL_STRING_SCHEMA)
      .field("incident_reserve_down_indicator", Schema.OPTIONAL_STRING_SCHEMA)
      .field("min_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("mid_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("max_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build()
  }

}
