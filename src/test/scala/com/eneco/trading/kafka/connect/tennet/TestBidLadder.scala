package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId

import org.apache.kafka.connect.data.Struct

/**
  * Created by jhofman on 23/02/2017.
  */
class TestBidLadder extends TestBase {
  val sourceType = SourceType(SourceName.BID_LADDER_TOTAL_NAME, "topic", "http://localhost/", ZoneId.of("Europe/Amsterdam"), 0, 4)

  test("Bid Ladder makes records correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = BidLadderSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)

    val records = uut.produce
    records.head.value shouldBe new Struct(TennetSourceConfig.SCHEMA_BIDLADDER)
      .put("date", "2017-02-24T00:00:00")
      .put("ptu", 1.toLong)
      .put("period_from", "00:00")
      .put("period_until", "00:15")
      .put("total_rampdown_required", 1.0)
      .put("rampdown_required", 2.0)
      .put("rampdown_reserve", 3.0)
      .put("rampdown_power", null)
      .put("rampup_power", 5.0)
      .put("rampup_reserve",null)
      .put("rampup_required", 7.0)
      .put("total_rampup_required", 8.0)
      .put("generated_at", EpochMillis("2017-01-01T11:06:00+01:00"))
      .put("ptu_start", EpochMillis("2017-02-24T00:00:00+01:00"))
  }

  val xml1 =
    """
      |<LadderSize xmlns="">
      |  <Record>
      |    <DATE>2017-02-24T00:00:00</DATE>
      |    <PTU>1</PTU>
      |    <PERIOD_FROM>00:00</PERIOD_FROM>
      |    <PERIOD_UNTIL>00:15</PERIOD_UNTIL>
      |    <TOTAL_RAMPDOWN_REQUIRED>1</TOTAL_RAMPDOWN_REQUIRED>
      |    <RAMPDOWN_REQUIRED>2</RAMPDOWN_REQUIRED>
      |    <RAMPDOWN_RESERVE>3</RAMPDOWN_RESERVE>
      |    <RAMPUP_POWER>5</RAMPUP_POWER>
      |    <RAMPUP_RESERVE/>
      |    <RAMPUP_REQUIRED>7</RAMPUP_REQUIRED>
      |    <TOTAL_RAMPUP_REQUIRED>8</TOTAL_RAMPUP_REQUIRED>
      |  </Record>
      |</LadderSize>
    """.stripMargin
}
