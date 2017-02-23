package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId

import org.apache.commons.codec.digest.DigestUtils

import scala.collection.JavaConverters._

/**
  * Created by jhofman on 23/02/2017.
  */
class TestImbalancePrice extends TestBase {
  val sourceType = SourceType(SourceName.IMBALANCE_PRICE_NAME, "topic", "http://localhost/", ZoneId.of("Europe/Amsterdam"), 0, 4)

  test("Imbalance Price no records") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = ImbalancePriceSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = None
    uut.produce.size shouldBe 0
  }

  test("Imbalance Price asks for correct days") {
    var mock = new MockServiceProvider("2017-01-05T11:06:00+01:00")
    var uut = ImbalancePriceSourceRecordProducer(mock, SourceType(SourceName.IMBALANCE_PRICE_NAME, "topic", "http://localhost/", ZoneId.of("Europe/Amsterdam"), 0, 4))

    uut.produce.size shouldBe 0
    mock.mockXmlReader.queries.toList shouldBe List(
      "http://localhost/imbalanceprice/20170101.xml",
      "http://localhost/imbalanceprice/20170102.xml",
      "http://localhost/imbalanceprice/20170103.xml",
      "http://localhost/imbalanceprice/20170104.xml",
      "http://localhost/imbalanceprice/20170105.xml"
    )

    mock = new MockServiceProvider("2017-01-10T11:06:00+01:00")
    uut = ImbalancePriceSourceRecordProducer(mock, SourceType(SourceName.IMBALANCE_PRICE_NAME, "topic", "http://anotherhost/", ZoneId.of("Europe/Amsterdam"), 3, 0))

    uut.produce.size shouldBe 0
    mock.mockXmlReader.queries.toList shouldBe List(
      "http://anotherhost/imbalanceprice/20170110.xml",
      "http://anotherhost/imbalanceprice/20170111.xml",
      "http://anotherhost/imbalanceprice/20170112.xml",
      "http://anotherhost/imbalanceprice/20170113.xml"
    )

    mock = new MockServiceProvider("2017-01-15T11:06:00+01:00")
    uut = ImbalancePriceSourceRecordProducer(mock, SourceType(SourceName.IMBALANCE_PRICE_NAME, "topic", "http://localhost/a/path/", ZoneId.of("Europe/Amsterdam"), 0, 0))

    uut.produce.size shouldBe 0
    mock.mockXmlReader.queries.toList shouldBe List(
      "http://localhost/a/path/imbalanceprice/20170115.xml"
    )
  }

  test("Imbalance Price sets topic correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    def uut(topic: String) = ImbalancePriceSourceRecordProducer(
      mock,
      SourceType(SourceName.BALANCE_DELTA_NAME,
        topic, "", ZoneId.of("Europe/Amsterdam"), 0, 0)
    )

    mock.mockXmlReader.content = Some(xml1)
    uut("a_topic").produce.head.topic() shouldBe "a_topic"
    uut("other_topic").produce.head.topic() shouldBe "other_topic"
  }

  test("Imbalance Price sets sourcePartition correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = ImbalancePriceSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.head.sourcePartition() shouldBe Map("source" -> SourceName.IMBALANCE_PRICE_NAME).asJava
    records.tail.head.sourcePartition() shouldBe Map("source" -> SourceName.IMBALANCE_PRICE_NAME).asJava
  }

  test("Imbalance Price sets sourceOffset correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = ImbalancePriceSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.size shouldBe 5
    records.head.sourceOffset() shouldBe
      Map(
        "20161228" -> DigestUtils.sha256Hex(xml1)
      ).asJava
    records.tail.head.sourceOffset() shouldBe
      Map(
        "20161228" -> DigestUtils.sha256Hex(xml1),
        "20161229" -> DigestUtils.sha256Hex(xml1)
      ).asJava
  }

  test("Imbalance Price make records correctly") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = ImbalancePriceSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some(xml1)
    val records = uut.produce
    records.head.value() shouldBe ImbalancePriceSourceRecord.struct(
      ImbalancePriceSourceRecord(
        "2017-02-22T00:00:00", 1,
        "00:00",
        "00:15",
        0, 1, 2, 3, 4, 5, 6, 7,
        EpochMillis("2017-01-01T11:06:00+01:00"),
        EpochMillis("2017-02-22T00:00:00+01:00")
      )
    )
  }

  test("Imbalance Price should handle bad xml") {
    val mock = new MockServiceProvider("2017-01-01T11:06:00+01:00")
    val uut = ImbalancePriceSourceRecordProducer(mock, sourceType)

    mock.mockXmlReader.content = Some("Not xml")
    uut.produce.size shouldBe 0
  }

  val xml1 =
    """
      |<ImbalancePrice xmlns="">
      |  <Record>
      |    <DATE>2017-02-22T00:00:00</DATE>
      |    <PTU>1</PTU>
      |    <PERIOD_FROM>00:00</PERIOD_FROM>
      |    <PERIOD_UNTIL>00:15</PERIOD_UNTIL>
      |    <UPWARD_INCIDENT_RESERVE/>
      |    <DOWNWARD_INCIDENT_RESERVE>1</DOWNWARD_INCIDENT_RESERVE>
      |    <UPWARD_DISPATCH>2</UPWARD_DISPATCH>
      |    <DOWNWARD_DISPATCH>3</DOWNWARD_DISPATCH>
      |    <INCENTIVE_COMPONENT>4</INCENTIVE_COMPONENT>
      |    <TAKE_FROM_SYSTEM>5</TAKE_FROM_SYSTEM>
      |    <FEED_INTO_SYSTEM>6</FEED_INTO_SYSTEM>
      |    <REGULATION_STATE>7</REGULATION_STATE>
      |  </Record>
      |</ImbalancePrice>
    """.stripMargin

}


