package com.eneco.trading.kafka.connect.tennet

import java.time.{Duration, Instant}

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class TennetConnectorTest extends FunSuite with Matchers with BeforeAndAfter with StrictLogging {

  val xml = scala.xml.XML.loadString(TestData.xmlString)

  test("parse xml") {
    val imbalance = scala.xml.XML.loadString(TestData.xmlString)
    assert((imbalance \\ "RECORD").length == 2)
  }

  test("testing empty doubles") {
    val midprice = TennetHelper.NodeSeqToDouble((xml \\ "RECORD").head \\ "MID_PRICE").getOrElse(0)
    val maxprice = TennetHelper.NodeSeqToDouble((xml \\ "RECORD").head \\ "MMAX_PRICE").getOrElse(0)
    assert(midprice == 35.14)
    assert(maxprice == 0)
  }

  test("two backoff failures") {
    println("Start Tennet Connector with: ")
    val interval = Duration.parse("PT1M")
    val maxBackOff = Duration.parse("PT40M")
    var backoff = new ExponentialBackOff(interval, maxBackOff)
    backoff = backoff.nextFailure().nextFailure()
    assert(Instant.now.plus(Duration.parse("PT4M")).getEpochSecond ==
      backoff.endTime.getEpochSecond)
  }

  test("create source test") {
    val cfg = TestData.connectConfiguration
    val imbTopic = cfg.getString(TennetSourceConfig.IMBALANCE_TOPIC)

    val srcTypes: Seq[SourceType] = TennetSourceTypes.createSources(cfg)

    assert(srcTypes.size == 4)
    assert(srcTypes.count(_.name == "balancedelta2017") == 1)
    assert(srcTypes.count(_.topic == imbTopic) == 1)
  }

  test("TennetSourceRecordProducer") {
    val mock = new MockOffsetStorageReader
    val producer = TennetSourceRecordProducer(mock)
    val records = producer.produce(TestData.balanceDeltaSourceType)
    assert(records.size==2)
  }

  test("TennetXML offset") {
    val offsetStorageReader = new MockOffsetStorageReader
    lazy val tennetXml = TennetImbalanceXml(offsetStorageReader, TestData.balanceDeltaSourceType)
    val records: Seq[SourceRecord] = tennetXml.produce
    assert(records.size==0)
  }
}







