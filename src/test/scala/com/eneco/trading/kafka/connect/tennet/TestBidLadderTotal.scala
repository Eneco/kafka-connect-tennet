package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId

/**
  * Created by jhofman on 23/02/2017.
  */
class TestBidLadderTotal extends TestBase {
  val sourceType = SourceType(SourceName.BID_LADDER_TOTAL_NAME, "topic", "http://localhost/", ZoneId.of("Europe/Amsterdam"), 0, 4)

  test("Bid Ladder Total makes records correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = BidLadderTotalSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.head.value() shouldBe BidLadderTotalSourceRecord.struct(
      BidLadderTotalSourceRecord(
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
      |    <RAMPDOWN_60_>1</RAMPDOWN_60_>
      |    <RAMPDOWN_15_60>2</RAMPDOWN_15_60>
      |    <RAMPDOWN_0_15>3</RAMPDOWN_0_15>
      |    <RAMPUP_0_15>4</RAMPUP_0_15>
      |    <RAMPUP_15_60>5</RAMPUP_15_60>
      |    <RAMPUP_60_240>6</RAMPUP_60_240>
      |    <RAMPUP_240_480>7</RAMPUP_240_480>
      |    <RAMPUP_480_>8</RAMPUP_480_>
      |  </Record>
      |</LadderSize>
    """.stripMargin
}
