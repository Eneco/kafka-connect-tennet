package com.eneco.trading.kafka.connect.tennet

import java.time.{Duration, Instant}
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.storage.OffsetStorageReader
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
    assert(srcTypes.count(_.name == "balansdelta2017") == 1)
    assert(srcTypes.count(_.topic == imbTopic) == 1)
  }

  test("TennetSourceRecordProducer") {
    val mock = new MockOffsetStorageReader
    val producer = TennetSourceRecordProducer(mock)
    val record = BidLadderTennetRecord
    //val tennetXml = TennetBidladderXml(mock, "http://www.tennet.org/", isIntraday = true)
    //val recs = TestData.bidLadderRecord.map(tennetXml.mapRecord(_))
  }
}

class MockOffsetStorageReader extends OffsetStorageReader {
  override def offset[T](partition: util.Map[String, T]): util.Map[String, AnyRef] = {
    null
  }

  override def offsets[T](partitions: util.Collection[util.Map[String, T]]): util.Map[util.Map[String, T], util.Map[String, AnyRef]] = {
    null
  }
}





