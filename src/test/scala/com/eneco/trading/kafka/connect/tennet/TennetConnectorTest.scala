package com.eneco.trading.kafka.connect.tennet

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDate}
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.JavaConverters._
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.util.{Failure, Try}
import scala.xml.Elem

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

  test("parse config") {
    println("parsing config: ")
    val props = Map(
      "connector.class" -> "com.eneco.trading.kafka.connect.tennet.TennetSourceConnector",
      "url" -> "http://www.tennet.org/xml/",
      "tasks.max" -> "1",
      "interval" -> "10000",
      "tennet.balance.delta.topic" -> "tennet_imbalancedelta",
      "tennet.imbalance.topic" -> "tennet_settlementprice",
      "tennet.bid.ladder.topic" -> "tennet_bidladder",
      "tennet.bid.ladder.total.topic" -> "tennet_bidladdertotal"
    )
    println(props.asJava.toString())
    val sourceConfig = new TennetSourceConfig(props.asJava)
  }

  test("Two backoff failures") {
    println("Start Tennet Connector with: ")
    val interval = Duration.parse("PT1M")
    val maxBackOff = Duration.parse("PT40M")
    var backoff = new ExponentialBackOff(interval, maxBackOff)
    backoff = backoff.nextFailure().nextFailure()
    assert(Instant.now.plus(Duration.parse("PT4M")).getEpochSecond ==
      backoff.endTime.getEpochSecond)
  }

  test("TennetBidladderXml") {

    //    val dataDayAhead = TennetBidladderXml(offsetStorageReader, url, isIntraday = true)
    //    val da= dataDayAhead.fromBody().map(r =>
    //      new SourceRecord(
    //        dataDayAhead.connectPartition, //source partitions?
    //        dataDayAhead.connectOffsetFromRecord(r),
    //        topic,
    //        BidLadderSourceRecord.schema,
    //        BidLadderSourceRecord.struct(r))
    //    )
    //    id ++ da
  }

  test("TennetSourceRecordProducer") {
    val mock = new MockOffsetStorageReader
    val producer = TennetSourceRecordProducer(mock)
    val record = BidLadderRecord
    val tennetXml = TennetBidladderXml(mock, "http://www.tennet.org/", isIntraday = true)
    val recs = TestData.bidLadderRecord.map(tennetXml.mapRecord(_))
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





