package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId

/**
  * Created by jhofman on 23/02/2017.
  */
class TestPriceLadder extends TestBase {
  val sourceType = SourceType(SourceName.PRICE_LADDER_NAME, "topic", "http://localhost/", ZoneId.of("Europe/Amsterdam"), 0, 4)

  test("Price Ladder makes records correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = PriceLadderSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.head.value() shouldBe PriceLadderSourceRecord.struct(
      PriceLadderSourceRecord(
        "2017-02-22T00:00:00", 1,
        "00:00",
        "00:15",
        1, 2, 3, 4, None, None, 7, 8, 9, 10, 11, 12,
        EpochMillis("2017-01-01T11:06:00+01:00"),
        EpochMillis("2017-02-22T00:00:00+01:00")
      )
    )
  }

  val xml1 =
    """
      |<PriceLadder xmlns="">
      |  <Record>
      |    <DATE>2017-02-22T00:00:00</DATE>
      |    <PTU>1</PTU>
      |    <PERIOD_FROM>00:00</PERIOD_FROM>
      |    <PERIOD_UNTIL>00:15</PERIOD_UNTIL>
      |    <NEG_TOTAL>1</NEG_TOTAL>
      |    <NEG_MAX>2</NEG_MAX>
      |    <NEG_600>3</NEG_600>
      |    <NEG_300>4</NEG_300>
      |    <NEG_100/>
      |    <POS_MIN>7</POS_MIN>
      |    <POS_100>8</POS_100>
      |    <POS_300>9</POS_300>
      |    <POS_600>10</POS_600>
      |    <POS_MAX>11</POS_MAX>
      |    <POS_TOTAL>12</POS_TOTAL>
      |  </Record>
      |</PriceLadder>
    """.stripMargin
}
