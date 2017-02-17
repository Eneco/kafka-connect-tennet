package com.eneco.trading.kafka.connect.tennet

import java.time.{ZoneId, ZonedDateTime, Instant}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.slf4j.StrictLogging

/**
  * Created by john hofman on 16/02/2017.
  */
case class EpochMillis(zone: ZoneId) extends StrictLogging {

  val PTU_MINUTES = 15;
  val WRAP_THRESHOLD_MINUTES = 12 * 60;

  /**
    * This converts a local datetime string and ptu index into a epoch millis. Any
    * time information from the datetime is ignored.
    *
    * @param datetime A datetime string with 'YYYY-MM-DDTHH:mm:ss' format. Not timezone info.
    * @param ptu The index of the ptu, starting at 1
    * @return epochmillis
    */
  def fromPTU(datetime: String, ptu: Int): Long = {
    ZonedDateTime.parse(datetime, DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(zone))
      .truncatedTo(ChronoUnit.DAYS)
      .plusMinutes((ptu - 1) * PTU_MINUTES)
      .toInstant.toEpochMilli
  }

  /**
    * This converts a simple minute timestamp to epoch milliseconds.
    *
    * @param now Sample time in epoch millis
    * @param minutes Minute of day with format 'HH:mm'
    * @return Epoch millis of the minute timestamp
    */
  def fromMinutes(now: Long, minutes: String): Long = {
    val localInstant = Instant.ofEpochMilli(now).atZone(zone)

    val localMinutes = localInstant.getHour() * 60 + localInstant.getMinute()
    val recordMinutes = minutes.substring(0,2).toInt * 60 + minutes.substring(3,5).toInt
    val wrappedDays = if (localMinutes < recordMinutes - WRAP_THRESHOLD_MINUTES) 1 else 0

    localInstant
      .truncatedTo(ChronoUnit.DAYS)
      .minusDays(wrappedDays)
      .plusMinutes(recordMinutes)
      .toInstant.toEpochMilli
  }

}
