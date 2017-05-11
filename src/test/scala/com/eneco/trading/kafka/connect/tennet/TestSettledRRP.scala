package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId

import org.apache.kafka.connect.data.Struct


class TestSettledRRP extends TestBase {
  val sourceType = SourceType(SourceName.SETTLED_RRP_NAME, "topic", "http://localhost/", ZoneId.of("Europe/Amsterdam"), 0, 4)

  test("Price Ladder makes records correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = PriceLadderSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.head.value shouldBe new Struct(TennetSourceConfig.SCHEMA_PRICELADDER)
      .put("date", "2017-02-22T00:00:00")
      .put("ptu", 1.toLong)
      .put("period_from", "00:00")
      .put("period_until", "00:15")
      .put("neg_total", 1.0)
      .put("neg_max", 2.0)
      .put("neg_600", 3.0)
      .put("neg_300", 4.0)
      .put("neg_100", null)
      .put("neg_min", null)
      .put("pos_min", 7.0)
      .put("pos_100", 8.0)
      .put("pos_300", 9.0)
      .put("pos_600", 10.0)
      .put("pos_max", 11.0)
      .put("pos_total", 12.0)
      .put("generated_at", EpochMillis("2017-01-01T11:06:00+01:00"))
      .put("ptu_start", EpochMillis("2017-02-22T00:00:00+01:00"))
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



//===================================================================

package com.eneco.trading.kafka.connect.tennet

/**
  * Created by joshua on 11-5-17.
  */
class TestSettledRRP {

}
