package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId

import org.apache.kafka.connect.data.Struct


class TestSettledRRP extends TestBase {
  val sourceType = SourceType(SourceName.SETTLED_RRP_NAME, "topic", "http://localhost/", ZoneId.of("Europe/Amsterdam"), 0, 4)

  test("Settled RRP makes records correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = SettledRRPSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce

    records.head.value shouldBe new Struct(TennetSourceConfig.SCHEMA_SETTLED_RRP)
      .put("date", "2017-05-06T00:00:00")
      .put("ptu", 1.toLong)
      .put("period_from", "00:00")
      .put("period_until", "00:15")
      .put("downward_reserve", null)
      .put("downward_power", -15366.0)
      .put("downward_incident_reserve", null)
      .put("upward_reserve", null)
      .put("upward_power", 849.0)
      .put("upward_incident_reserve", null)
      .put("volume", 16215.0)
      .put("totals", -14517.0)
      .put("generated_at", EpochMillis("2017-01-01T11:06:00+01:00"))
      .put("ptu_start", EpochMillis("2017-05-06T00:00:00+02:00"))  // on 2017-05-06 there is +02:00 between cet and utc

  }

  val xml1 =
    """
      |<PastRecordOfRRP xmlns="">
      |  <Record>
      |    <DATE>2017-05-06T00:00:00</DATE>
      |    <PTU>1</PTU>
      |    <PERIOD_FROM>00:00</PERIOD_FROM>
      |    <PERIOD_UNTIL>00:15</PERIOD_UNTIL>
      |    <DOWNWARD_RESERVE/>
      |    <DOWNWARD_POWER>-15366</DOWNWARD_POWER>
      |    <UPWARD_POWER>849</UPWARD_POWER>
      |    <UPWARD_RESERVE/>
      |    <UPWARD_INCIDENT_RESERVE/>
      |    <VOLUME>16215</VOLUME>
      |    <TOTALS>-14517</TOTALS>
      |  </Record>
      |</PastRecordOfRRP>
    """.stripMargin
}
