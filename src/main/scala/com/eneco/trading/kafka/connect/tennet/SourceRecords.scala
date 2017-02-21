package com.eneco.trading.kafka.connect.tennet

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

object ImbalanceSourceRecord {
  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet.imbalance")
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
    .field("generated_at", Schema.OPTIONAL_INT64_SCHEMA)
    .field("value_time", Schema.OPTIONAL_INT64_SCHEMA)
    .build()

  def struct(record: ImbalanceTennetRecord) =
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
      .put("generated_at", record.GeneratedAt)
      .put("value_time", record.ValueTime)
}

object BidLadderSourceRecord {
  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet.bidladder")
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
    .field("generated_at", Schema.OPTIONAL_INT64_SCHEMA)
    .field("ptu_start", Schema.OPTIONAL_INT64_SCHEMA)
    .build()

  def struct(record: BidLadderTennetRecord) =
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
      .put("generated_at", record.GeneratedAt)
      .put("ptu_start", record.PtuStart)
}

object BidLadderTotalSourceRecord {
  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet.bidladdertotal")
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("rampdown_60", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_15_60", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_0_15", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_0_15", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_60_240", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_240_480", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_480", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.OPTIONAL_INT64_SCHEMA)
    .field("ptu_start", Schema.OPTIONAL_INT64_SCHEMA)
    .build()

  def struct(record: BidLadderTotalTennetRecord) =
    new Struct(schema)
      .put("date", record.Date)
      .put("ptu", record.PTU)
      .put("period_from", record.PeriodFrom)
      .put("period_until", record.PeriodUntil)
      .put("rampdown_60", record.Rampdown_60)
      .put("rampdown_15_60", record.Rampdown_15_60)
      .put("rampdown_0_15", record.Rampdown_0_15)
      .put("rampup_0_15", record.Rampup_0_15)
      .put("rampup_60_240", record.Rampup_60_240)
      .put("rampup_240_480", record.Rampup_240_480)
      .put("rampup_480", record.Rampup_480)
      .put("generated_at", record.GeneratedAt)
      .put("ptu_start", record.PtuStart)
}

object ImbalancePriceSourceRecord {
  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet.imbalanceprice")
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("upward_incident_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("downward_incident_reserve", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("upward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("downward_dispatch", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("incentive_component", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("take_from_system", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("feed_into_system", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("regulation_state", Schema.OPTIONAL_INT64_SCHEMA)
    .field("generated_at", Schema.OPTIONAL_INT64_SCHEMA)
    .field("ptu_start", Schema.OPTIONAL_INT64_SCHEMA)
    .build()

  def struct(record: ImbalancePriceTennetRecord) =
    new Struct(schema)
      .put("date", record.Date)
      .put("ptu", record.PTU)
      .put("period_from", record.PeriodFrom)
      .put("period_until", record.PeriodUntil)
      .put("upward_incident_reserve", record.UpwardIncidentReserve)
      .put("downward_incident_reserve", record.DownwardIncidentReserve)
      .put("upward_dispatch", record.UpwardDispatch)
      .put("downward_dispatch", record.DownwardDispatch)
      .put("incentive_component", record.IncentiveComponent)
      .put("take_from_system", record.TakeFromSystem)
      .put("feed_into_system", record.FeedIntoSystem)
      .put("regulation_state", record.RegulationState)
      .put("generated_at", record.GeneratedAt)
      .put("ptu_start", record.PtuStart)
}


case class BidLadderTennetRecord(
                            Date: String,
                            PTU: Long,
                            PeriodFrom: String,
                            PeriodUntil: String,
                            TotalRampDownRequired: Double,
                            RampDownRequired: Double,
                            RampDownReserve: Double,
                            RampDownPower: Double,
                            RampUpPower: Double,
                            RampUpReserve: Double,
                            RampUpRequired: Double,
                            TotalRampUpRequired: Double,
                            GeneratedAt: Long,
                            PtuStart: Long
                          ) extends TennetSourceRecord

case class ImbalanceTennetRecord(
                            Number: Long,
                            SequenceNumber: Long,
                            Time: String,
                            IgcccontributionUp: Double,
                            IgcccontributionDown: Double,
                            UpwardDispatch: Double,
                            DownwardDispatch: Double,
                            ReserveUpwardDispatch: Double,
                            ReserveDownwardDispatch: Double,
                            IncidentReserveUpIndicator: String,
                            IncidentReserveDownIndicator: String,
                            MinPrice: Double,
                            MidPrice: Double,
                            MaxPrice: Double,
                            GeneratedAt: Long,
                            ValueTime: Long
                          ) extends TennetSourceRecord

case class BidLadderTotalTennetRecord(
                                             Date: String,
                                             PTU: Long,
                                             PeriodFrom: String,
                                             PeriodUntil: String,
                                             Rampdown_60: Double,
                                             Rampdown_0_15: Double,
                                             Rampdown_15_60: Double,
                                             Rampup_0_15: Double,
                                             Rampup_60_240: Double,
                                             Rampup_240_480: Double,
                                             Rampup_480: Double,
                                             GeneratedAt: Long,
                                             PtuStart: Long
                                           ) extends TennetSourceRecord


case class ImbalancePriceTennetRecord(
                                             Date: String,
                                             PTU: Long,
                                             PeriodFrom: String,
                                             PeriodUntil: String,
                                             UpwardIncidentReserve: Double,
                                             DownwardIncidentReserve: Double,
                                             UpwardDispatch: Double,
                                             DownwardDispatch: Double,
                                             IncentiveComponent: Double,
                                             TakeFromSystem: Double,
                                             FeedIntoSystem: Double,
                                             RegulationState: Long,
                                             GeneratedAt: Long,
                                             PtuStart: Long
                                           ) extends TennetSourceRecord
