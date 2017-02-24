package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId

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
    records.head.value() shouldBe BidLadderSourceRecord.struct(
      BidLadderSourceRecord(
        "2017-02-24T00:00:00", 1,
        "00:00",
        "00:15",
        1, 2, 3, 4, 5, 6, 7, 8,
        EpochMillis("2017-01-01T11:06:00+01:00"),
        EpochMillis("2017-02-24T00:00:00+01:00")
      )
    )
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
      |    <RAMPDOWN_POWER>4</RAMPDOWN_POWER>
      |    <RAMPUP_POWER>5</RAMPUP_POWER>
      |    <RAMPUP_RESERVE>6</RAMPUP_RESERVE>
      |    <RAMPUP_REQUIRED>7</RAMPUP_REQUIRED>
      |    <TOTAL_RAMPUP_REQUIRED>8</TOTAL_RAMPUP_REQUIRED>
      |  </Record>
      |</LadderSize>
    """.stripMargin
}
