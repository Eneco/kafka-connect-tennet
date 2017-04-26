package com.eneco.trading.kafka.connect.tennet

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

object BalanceDeltaSourceRecord {
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
    .field("incident_reserve_up_indicator", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("incident_reserve_down_indicator", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("min_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("mid_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("max_price", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("value_time", Schema.INT64_SCHEMA)
    .build()

  def struct(record: BalanceDeltaSourceRecord) =
    new Struct(schema)
      .put("number", record.Number)
      .put("sequence_number", record.SequenceNumber)
      .put("time", record.Time)
      .put("igcccontribution_up", record.IgcccontributionUp.getOrElse(null))
      .put("igcccontribution_down", record.IgcccontributionDown.getOrElse(null))
      .put("upward_dispatch", record.UpwardDispatch.getOrElse(null))
      .put("downward_dispatch", record.DownwardDispatch.getOrElse(null))
      .put("reserve_upward_dispatch", record.ReserveUpwardDispatch.getOrElse(null))
      .put("reserve_downward_dispatch", record.ReserveDownwardDispatch.getOrElse(null))
      .put("incident_reserve_up_indicator", record.IncidentReserveUpIndicator.getOrElse(null))
      .put("incident_reserve_down_indicator", record.IncidentReserveDownIndicator.getOrElse(null))
      .put("reserve_upward_dispatch", record.ReserveUpwardDispatch.getOrElse(null))
      .put("reserve_downward_dispatch", record.ReserveDownwardDispatch.getOrElse(null))
      .put("min_price", record.MinPrice.getOrElse(null))
      .put("mid_price", record.MidPrice.getOrElse(null))
      .put("max_price", record.MaxPrice.getOrElse(null))
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
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  def struct(record: BidLadderSourceRecord) =
    new Struct(schema)
      .put("date", record.Date)
      .put("ptu", record.PTU)
      .put("period_from", record.PeriodFrom)
      .put("period_until", record.PeriodUntil)
      .put("total_rampdown_required", record.TotalRampDownRequired.getOrElse(null))
      .put("rampdown_required", record.RampDownRequired.getOrElse(null))
      .put("rampdown_reserve", record.RampDownReserve.getOrElse(null))
      .put("rampdown_power", record.RampDownPower.getOrElse(null))
      .put("rampup_power", record.RampUpPower.getOrElse(null))
      .put("rampup_reserve", record.RampUpReserve.getOrElse(null))
      .put("rampup_required", record.RampUpRequired.getOrElse(null))
      .put("total_rampup_required", record.TotalRampUpRequired.getOrElse(null))
      .put("generated_at", record.GeneratedAt)
      .put("ptu_start", record.PtuStart)
}

object BidLadderTotalSourceRecord{
  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet.bidladdertotal")
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("rampdown_60", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_15_60", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampdown_0_15", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_0_15", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_15_60", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_60_240", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_240_480", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("rampup_480", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  def struct(record: BidLadderTotalSourceRecord) =
    new Struct(schema)
      .put("date", record.Date)
      .put("ptu", record.PTU)
      .put("period_from", record.PeriodFrom)
      .put("period_until", record.PeriodUntil)
      .put("rampdown_60", record.Rampdown_60.getOrElse(null))
      .put("rampdown_15_60", record.Rampdown_15_60.getOrElse(null))
      .put("rampdown_0_15", record.Rampdown_0_15.getOrElse(null))
      .put("rampup_0_15", record.Rampup_0_15.getOrElse(null))
      .put("rampup_15_60", record.Rampup_15_60.getOrElse(null))
      .put("rampup_60_240", record.Rampup_60_240.getOrElse(null))
      .put("rampup_240_480", record.Rampup_240_480.getOrElse(null))
      .put("rampup_480", record.Rampup_480.getOrElse(null))
      .put("generated_at", record.GeneratedAt)
      .put("ptu_start", record.PtuStart)
}

object ImbalancePriceSourceRecord{
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
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  def struct(record: ImbalancePriceSourceRecord) =
    new Struct(schema)
      .put("date", record.Date)
      .put("ptu", record.PTU)
      .put("period_from", record.PeriodFrom)
      .put("period_until", record.PeriodUntil)
      .put("upward_incident_reserve", record.UpwardIncidentReserve.getOrElse(null))
      .put("downward_incident_reserve", record.DownwardIncidentReserve.getOrElse(null))
      .put("upward_dispatch", record.UpwardDispatch.getOrElse(null))
      .put("downward_dispatch", record.DownwardDispatch.getOrElse(null))
      .put("incentive_component", record.IncentiveComponent.getOrElse(null))
      .put("take_from_system", record.TakeFromSystem.getOrElse(null))
      .put("feed_into_system", record.FeedIntoSystem.getOrElse(null))
      .put("regulation_state", record.RegulationState)
      .put("generated_at", record.GeneratedAt)
      .put("ptu_start", record.PtuStart)
}


object PriceLadderSourceRecord {
  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet.priceladder")
    .field("date", Schema.STRING_SCHEMA)
    .field("ptu", Schema.INT64_SCHEMA)
    .field("period_from", Schema.STRING_SCHEMA)
    .field("period_until", Schema.STRING_SCHEMA)
    .field("neg_total", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_max", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_600", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_300", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_100", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("neg_min", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_min", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_100", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_300", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_600", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_max", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("pos_total", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("generated_at", Schema.INT64_SCHEMA)
    .field("ptu_start", Schema.INT64_SCHEMA)
    .build()

  def struct(record: PriceLadderSourceRecord) =
    new Struct(schema)
      .put("date", record.Date)
      .put("ptu", record.PTU)
      .put("period_from", record.PeriodFrom)
      .put("period_until", record.PeriodUntil)
      .put("neg_total", record.NegTotal.getOrElse(null))
      .put("neg_max", record.MegMax.getOrElse(null))
      .put("neg_600", record.Neg600.getOrElse(null))
      .put("neg_300", record.Neg300.getOrElse(null))
      .put("neg_100", record.Neg100.getOrElse(null))
      .put("neg_min", record.NegMin.getOrElse(null))
      .put("pos_min", record.PosMin.getOrElse(null))
      .put("pos_100", record.Pos100.getOrElse(null))
      .put("pos_300", record.Pos300.getOrElse(null))
      .put("pos_600", record.Pos600.getOrElse(null))
      .put("pos_max", record.PosMax.getOrElse(null))
      .put("pos_total", record.PosTotal.getOrElse(null))
      .put("generated_at", record.GeneratedAt)
      .put("ptu_start", record.PtuStart)
}


case class BidLadderSourceRecord(
                            Date: String,
                            PTU: Long,
                            PeriodFrom: String,
                            PeriodUntil: String,
                            TotalRampDownRequired: Option[Double],
                            RampDownRequired: Option[Double],
                            RampDownReserve: Option[Double],
                            RampDownPower: Option[Double],
                            RampUpPower: Option[Double],
                            RampUpReserve: Option[Double],
                            RampUpRequired: Option[Double],
                            TotalRampUpRequired: Option[Double],
                            GeneratedAt: Long,
                            PtuStart: Long
                          )

case class BalanceDeltaSourceRecord(
                            Number: Long,
                            SequenceNumber: Long,
                            Time: String,
                            IgcccontributionUp: Option[Double],
                            IgcccontributionDown: Option[Double],
                            UpwardDispatch: Option[Double],
                            DownwardDispatch: Option[Double],
                            ReserveUpwardDispatch: Option[Double],
                            ReserveDownwardDispatch: Option[Double],
                            IncidentReserveUpIndicator: Option[Double],
                            IncidentReserveDownIndicator: Option[Double],
                            MinPrice: Option[Double],
                            MidPrice: Option[Double],
                            MaxPrice: Option[Double],
                            GeneratedAt: Long,
                            ValueTime: Long
                          )

case class BidLadderTotalSourceRecord(
                                             Date: String,
                                             PTU: Long,
                                             PeriodFrom: String,
                                             PeriodUntil: String,
                                             Rampdown_60: Option[Double],
                                             Rampdown_0_15: Option[Double],
                                             Rampdown_15_60: Option[Double],
                                             Rampup_0_15: Option[Double],
                                             Rampup_15_60: Option[Double],
                                             Rampup_60_240: Option[Double],
                                             Rampup_240_480: Option[Double],
                                             Rampup_480: Option[Double],
                                             GeneratedAt: Long,
                                             PtuStart: Long
                                           )


case class ImbalancePriceSourceRecord(
                                             Date: String,
                                             PTU: Long,
                                             PeriodFrom: String,
                                             PeriodUntil: String,
                                             UpwardIncidentReserve: Option[Double],
                                             DownwardIncidentReserve: Option[Double],
                                             UpwardDispatch: Option[Double],
                                             DownwardDispatch: Option[Double],
                                             IncentiveComponent: Option[Double],
                                             TakeFromSystem: Option[Double],
                                             FeedIntoSystem: Option[Double],
                                             RegulationState: Long,
                                             GeneratedAt: Long,
                                             PtuStart: Long
                                           )



case class PriceLadderSourceRecord(
                                       Date: String,
                                       PTU: Long,
                                       PeriodFrom: String,
                                       PeriodUntil: String,
                                       NegTotal: Option[Double],
                                       MegMax: Option[Double],
                                       Neg600: Option[Double],
                                       Neg300: Option[Double],
                                       Neg100: Option[Double],
                                       NegMin: Option[Double],
                                       PosMin: Option[Double],
                                       Pos100: Option[Double],
                                       Pos300: Option[Double],
                                       Pos600: Option[Double],
                                       PosMax: Option[Double],
                                       PosTotal: Option[Double],
                                       GeneratedAt: Long,
                                       PtuStart: Long
                                     )