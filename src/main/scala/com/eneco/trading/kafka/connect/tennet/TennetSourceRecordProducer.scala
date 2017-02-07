package com.eneco.trading.kafka.connect.tennet


import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scalaj.http.{Http, HttpResponse}


case class TennetSourceRecordProducer(offsetStorageReader: OffsetStorageReader) extends StrictLogging {

  var partition = "imbalance"

  def produce(topic: String, url :String): Seq[SourceRecord] = {
    val response: HttpResponse[String] = Http(url).asString
    val data = TennetImbalanceXml(offsetStorageReader, response.body)
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
    def struct(record: ImbalanceRecord) =
      new Struct(schema)
        .put("number", record.Number)
        .put("sequence_number", record.SequenceNumber)
        .put("time", record.Time)
        .put("igcccontribution_up", record.IgcccontributionUp)
        .put("igcccontribution_down", record.IgcccontributionDown)
        .put("upward_dispatch", record.UpwardDispatch)
        .put("downward_dispatch", record.DownwardDispatch)
        .put("reserve_upward_dispatch", record.ReserveUpwardDispatch)
        .put("reserve_downward_dispatch", record.ReserveDownwardDispatch)
        .put("incident_reserve_up_indicator", record.IncidentReserveUpIndicator)
        .put("incident_reserve_down_indicator", record.IncidentReserveUpIndicator)
        .put("reserve_upward_dispatch", record.ReserveUpwardDispatch)
        .put("reserve_downward_dispatch", record.ReserveDownwardDispatch)
        .put("min_price", record.MinPrice)
        .put("mid_price", record.MidPrice)
        .put("max_price", record.MaxPrice)


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

  object BidLadderSourceRecord {
    def struct(record: BidLadderRecord) =
      new Struct(schema)
        .put("date", record.Date)
        .put("ptu", record.PTU)
        .put("period_from", record.PeriodFrom)
        .put("period_until", record.PeriodUntil)
        .put("total_rampdown_required", record.TotalRampDownRequired)
        .put("rampdown_required", record.RampDownRequired)
        .put("rampdown_reserve", record.RampDownReserve)
        .put("rampdown_power", record.RampDownPower)
        .put("rampup_power", record.RampUpPower)
        .put("rampup_reserve", record.RampUpReserve)
        .put("rampup_required", record.RampUpRequired)
        .put("total_rampup_required", record.TotalRampUpRequired)
        .put("generated_at",record.GeneratedAt)


    val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet")
      .field("date", Schema.STRING_SCHEMA)
      .field("ptu", Schema.INT64_SCHEMA)
      .field("period_from", Schema.STRING_SCHEMA)
      .field("period_until", Schema.STRING_SCHEMA)
      .field("total_rampdown_required", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("rampdown_required", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("rampdown_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("rampdown_power", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("rampup_power", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("rampup_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("rampup_required", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("total_rampup_required", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("generated_at", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build()
  }
}
