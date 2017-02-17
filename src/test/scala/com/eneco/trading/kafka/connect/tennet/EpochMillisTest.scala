package com.eneco.trading.kafka.connect.tennet

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Created by jhofman on 16/02/2017.
  */
class EpochMillisTest extends FunSuite with Matchers with BeforeAndAfter with StrictLogging {

  val amsterdam = ZoneId.of("Europe/Amsterdam")
  val auckland = ZoneId.of("Pacific/Auckland")

  def epoch(t: String) = ZonedDateTime.parse(t, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli

  test("fromPTU handles time zones") {
    EpochMillis(amsterdam).fromPTU("2017-01-02T00:00:00", 1) shouldBe epoch("2017-01-02T00:00:00+01:00")
    EpochMillis(auckland).fromPTU("2017-01-01T00:00:00", 1) shouldBe epoch("2017-01-01T00:00:00+13:00")
  }

  test("fromPTU handles ignores time in date string") {
    EpochMillis(amsterdam).fromPTU("2017-01-02T12:00:00", 1) shouldBe epoch("2017-01-02T00:00:00+01:00")
  }

  test("fromPTU handles ptus") {
    EpochMillis(amsterdam).fromPTU("2017-01-02T00:00:00", 2) shouldBe epoch("2017-01-02T00:15:00+01:00")
    EpochMillis(amsterdam).fromPTU("2017-01-02T00:00:00", 5) shouldBe epoch("2017-01-02T01:00:00+01:00")
    EpochMillis(amsterdam).fromPTU("2017-01-02T00:00:00", 13) shouldBe epoch("2017-01-02T03:00:00+01:00")
    EpochMillis(amsterdam).fromPTU("2017-01-02T00:00:00", 97) shouldBe epoch("2017-01-03T00:00:00+01:00")
  }

  test("fromPTU handles summer time") {

    // CEST transition
    EpochMillis(amsterdam).fromPTU("2016-10-30T00:00:00", 11) shouldBe epoch("2016-10-30T02:30:00+02:00")
    EpochMillis(amsterdam).fromPTU("2016-10-30T00:00:00", 15) shouldBe epoch("2016-10-30T02:30:00+01:00")

    // CET
    EpochMillis(amsterdam).fromPTU("2016-10-31T00:00:00", 11) shouldBe epoch("2016-10-31T02:30:00+01:00")

  }

  test("fromMinutes local time near record time") {
    EpochMillis(amsterdam).fromMinutes(epoch("2017-01-01T00:00:00+01:00"), "00:00") shouldBe epoch("2017-01-01T00:00:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-01-01T00:01:00+01:00"), "00:01") shouldBe epoch("2017-01-01T00:01:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-01-01T12:00:00+01:00"), "12:00") shouldBe epoch("2017-01-01T12:00:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-01-01T23:59:00+01:00"), "23:59") shouldBe epoch("2017-01-01T23:59:00+01:00")
  }

  test("fromMinutes handles timezones") {
    EpochMillis(auckland).fromMinutes(epoch("2017-01-01T00:00:00+13:00"), "00:00") shouldBe epoch("2017-01-01T00:00:00+13:00")
    EpochMillis(auckland).fromMinutes(epoch("2017-01-01T00:01:00+13:00"), "00:01") shouldBe epoch("2017-01-01T00:01:00+13:00")
    EpochMillis(auckland).fromMinutes(epoch("2017-01-01T12:00:00+13:00"), "12:00") shouldBe epoch("2017-01-01T12:00:00+13:00")
    EpochMillis(auckland).fromMinutes(epoch("2017-01-01T23:59:00+13:00"), "23:59") shouldBe epoch("2017-01-01T23:59:00+13:00")
  }

  test("fromMinutes day break") {
    EpochMillis(amsterdam).fromMinutes(epoch("2017-02-05T00:00:00+01:00"), "23:00") shouldBe epoch("2017-02-04T23:00:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-02-05T00:00:00+01:00"), "23:15") shouldBe epoch("2017-02-04T23:15:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-02-05T00:00:00+01:00"), "23:30") shouldBe epoch("2017-02-04T23:30:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-02-05T00:00:00+01:00"), "23:45") shouldBe epoch("2017-02-04T23:45:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-02-05T00:00:00+01:00"), "00:00") shouldBe epoch("2017-02-05T00:00:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-02-05T00:00:00+01:00"), "00:15") shouldBe epoch("2017-02-05T00:15:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-02-05T00:00:00+01:00"), "00:30") shouldBe epoch("2017-02-05T00:30:00+01:00")
    EpochMillis(amsterdam).fromMinutes(epoch("2017-02-05T00:00:00+01:00"), "00:45") shouldBe epoch("2017-02-05T00:45:00+01:00")
  }

}
