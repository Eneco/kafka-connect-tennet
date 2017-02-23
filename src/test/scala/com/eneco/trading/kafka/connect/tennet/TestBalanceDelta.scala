package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId

import scala.collection.JavaConverters._

/**
  * Created by jhofman on 22/02/2017.
  */
class TestBalanceDelta extends TestBase {
  val sourceType = SourceType(SourceName.BALANCE_DELTA_NAME, "topic", "", ZoneId.of("Europe/Amsterdam"), 0, 0)

  test("Balance Delta no records") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = BalanceDeltaSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = None
    uut.produce.size shouldBe 0
  }

  test("Balance Delta offset tracking") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = BalanceDeltaSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    uut.produce.size shouldBe 2
    uut.produce.size shouldBe 0

    mock.mockXmlReader.content = Some(xml2)
    uut.produce.size shouldBe 1
    uut.produce.size shouldBe 0

    mock.mockXmlReader.content = Some(xml3)
    val records = uut.produce
    records.size shouldBe 1
    records.head.sourceOffset().size shouldBe 1
  }

  test("Balance Delta sets topic correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    def uut(topic: String) = BalanceDeltaSourceRecordProducer(
      mock,
      SourceType(SourceName.BALANCE_DELTA_NAME,
        topic, "", ZoneId.of("Europe/Amsterdam"), 0, 0)
    )

    mock.mockXmlReader.content = Some(xml1)
    uut("a_topic").produce.head.topic() shouldBe "a_topic"
    uut("other_topic").produce.head.topic() shouldBe "other_topic"
  }

  test("Balance Delta sets sourcePartition correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = BalanceDeltaSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.head.sourcePartition() shouldBe Map("source" -> SourceName.BALANCE_DELTA_NAME).asJava
    records.tail.head.sourcePartition() shouldBe Map("source" -> SourceName.BALANCE_DELTA_NAME).asJava
  }

  test("Balance Delta sets sourceOffset correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = BalanceDeltaSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.head.sourceOffset() shouldBe
      Map(
        EpochMillis("2017-01-01T11:04:00+01:00").toString -> EpochMillis("2017-01-01T11:06:00+01:00").toString
      ).asJava
    records.tail.head.sourceOffset() shouldBe
      Map(
        EpochMillis("2017-01-01T11:04:00+01:00").toString -> EpochMillis("2017-01-01T11:06:00+01:00").toString,
        EpochMillis("2017-01-01T11:03:00+01:00").toString -> EpochMillis("2017-01-01T11:06:00+01:00").toString
      ).asJava
  }

  test("Balance Delta make records correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = BalanceDeltaSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.head.value() shouldBe BalanceDeltaSourceRecord.struct(
      BalanceDeltaSourceRecord(
        1, 665, "11:04",
        1,2,3,4,5,6,7,8,9,10,11,
        EpochMillis("2017-01-01T11:06:00+01:00"),
        EpochMillis("2017-01-01T11:04:00+01:00")
      )
    )
  }

  test("Balance Delta should handle bad xml") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = BalanceDeltaSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some("Not xml")
    uut.produce.size shouldBe 0
  }

  val xml1 =
    """
      |<BALANCE_DELTA xmlns="">
      |  <RECORD>
      |    <NUMBER>1</NUMBER>
      |    <SEQUENCE_NUMBER>665</SEQUENCE_NUMBER>
      |    <TIME>11:04</TIME>
      |    <IGCCCONTRIBUTION_UP>1</IGCCCONTRIBUTION_UP>
      |    <IGCCCONTRIBUTION_DOWN>2</IGCCCONTRIBUTION_DOWN>
      |    <UPWARD_DISPATCH>3</UPWARD_DISPATCH>
      |    <DOWNWARD_DISPATCH>4</DOWNWARD_DISPATCH>
      |    <RESERVE_UPWARD_DISPATCH>5</RESERVE_UPWARD_DISPATCH>
      |    <RESERVE_DOWNWARD_DISPATCH>6</RESERVE_DOWNWARD_DISPATCH>
      |    <INCIDENT_RESERVE_UP_INDICATOR>7</INCIDENT_RESERVE_UP_INDICATOR>
      |    <INCIDENT_RESERVE_DOWN_INDICATOR>8</INCIDENT_RESERVE_DOWN_INDICATOR>
      |    <MIN_PRICE>9</MIN_PRICE>
      |    <MID_PRICE>10</MID_PRICE>
      |    <MAX_PRICE>11</MAX_PRICE>
      |  </RECORD>
      |  <RECORD>
      |    <NUMBER>2</NUMBER>
      |    <SEQUENCE_NUMBER>664</SEQUENCE_NUMBER>
      |    <TIME>11:03</TIME>
      |    <IGCCCONTRIBUTION_UP>245</IGCCCONTRIBUTION_UP>
      |    <IGCCCONTRIBUTION_DOWN>0</IGCCCONTRIBUTION_DOWN>
      |    <UPWARD_DISPATCH>0</UPWARD_DISPATCH>
      |    <DOWNWARD_DISPATCH>0</DOWNWARD_DISPATCH>
      |    <RESERVE_UPWARD_DISPATCH>0</RESERVE_UPWARD_DISPATCH>
      |    <RESERVE_DOWNWARD_DISPATCH>0</RESERVE_DOWNWARD_DISPATCH>
      |    <INCIDENT_RESERVE_UP_INDICATOR>0</INCIDENT_RESERVE_UP_INDICATOR>
      |    <INCIDENT_RESERVE_DOWN_INDICATOR>0</INCIDENT_RESERVE_DOWN_INDICATOR>
      |    <MID_PRICE>34.15</MID_PRICE>
      |  </RECORD>
      |</BALANCE_DELTA>
    """.stripMargin

  val xml2 =
    """
      |<BALANCE_DELTA xmlns="">
      |  <RECORD>
      |    <NUMBER>1</NUMBER>
      |    <SEQUENCE_NUMBER>666</SEQUENCE_NUMBER>
      |    <TIME>11:05</TIME>
      |    <IGCCCONTRIBUTION_UP>270</IGCCCONTRIBUTION_UP>
      |    <IGCCCONTRIBUTION_DOWN>0</IGCCCONTRIBUTION_DOWN>
      |    <UPWARD_DISPATCH>0</UPWARD_DISPATCH>
      |    <DOWNWARD_DISPATCH>0</DOWNWARD_DISPATCH>
      |    <RESERVE_UPWARD_DISPATCH>0</RESERVE_UPWARD_DISPATCH>
      |    <RESERVE_DOWNWARD_DISPATCH>0</RESERVE_DOWNWARD_DISPATCH>
      |    <INCIDENT_RESERVE_UP_INDICATOR>0</INCIDENT_RESERVE_UP_INDICATOR>
      |    <INCIDENT_RESERVE_DOWN_INDICATOR>0</INCIDENT_RESERVE_DOWN_INDICATOR>
      |    <MID_PRICE>34.15</MID_PRICE>
      |  </RECORD>
      |  <RECORD>
      |    <NUMBER>2</NUMBER>
      |    <SEQUENCE_NUMBER>665</SEQUENCE_NUMBER>
      |    <TIME>11:04</TIME>
      |    <IGCCCONTRIBUTION_UP>270</IGCCCONTRIBUTION_UP>
      |    <IGCCCONTRIBUTION_DOWN>0</IGCCCONTRIBUTION_DOWN>
      |    <UPWARD_DISPATCH>0</UPWARD_DISPATCH>
      |    <DOWNWARD_DISPATCH>0</DOWNWARD_DISPATCH>
      |    <RESERVE_UPWARD_DISPATCH>0</RESERVE_UPWARD_DISPATCH>
      |    <RESERVE_DOWNWARD_DISPATCH>0</RESERVE_DOWNWARD_DISPATCH>
      |    <INCIDENT_RESERVE_UP_INDICATOR>0</INCIDENT_RESERVE_UP_INDICATOR>
      |    <INCIDENT_RESERVE_DOWN_INDICATOR>0</INCIDENT_RESERVE_DOWN_INDICATOR>
      |    <MID_PRICE>34.15</MID_PRICE>
      |  </RECORD>
      |</BALANCE_DELTA>
    """.stripMargin

  val xml3 =
    """
      |<BALANCE_DELTA xmlns="">
      |  <RECORD>
      |    <NUMBER>1</NUMBER>
      |    <SEQUENCE_NUMBER>727</SEQUENCE_NUMBER>
      |    <TIME>12:06</TIME>
      |    <IGCCCONTRIBUTION_UP>270</IGCCCONTRIBUTION_UP>
      |    <IGCCCONTRIBUTION_DOWN>0</IGCCCONTRIBUTION_DOWN>
      |    <UPWARD_DISPATCH>0</UPWARD_DISPATCH>
      |    <DOWNWARD_DISPATCH>0</DOWNWARD_DISPATCH>
      |    <RESERVE_UPWARD_DISPATCH>0</RESERVE_UPWARD_DISPATCH>
      |    <RESERVE_DOWNWARD_DISPATCH>0</RESERVE_DOWNWARD_DISPATCH>
      |    <INCIDENT_RESERVE_UP_INDICATOR>0</INCIDENT_RESERVE_UP_INDICATOR>
      |    <INCIDENT_RESERVE_DOWN_INDICATOR>0</INCIDENT_RESERVE_DOWN_INDICATOR>
      |    <MID_PRICE>34.15</MID_PRICE>
      |  </RECORD>
      |</BALANCE_DELTA>
    """.stripMargin

}
