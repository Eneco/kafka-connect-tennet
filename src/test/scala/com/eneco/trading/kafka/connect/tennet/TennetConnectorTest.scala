package com.eneco.trading.kafka.connect.tennet

import java.time.{Clock, Duration, Instant, LocalDate}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.Files.copy
import java.nio.file.Paths.get
import java.time

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder

import scalaj.http.Http;

class TennetConnectorTest extends TestBase {

  val server = new Server(8899)
  val xml = scala.xml.XML.loadString(TestData.xmlString)

  override def beforeAll() {
    val  context = new ServletContextHandler()
    val  defaultServ = new ServletHolder("default", classOf[DefaultServlet])
    defaultServ.setInitParameter("resourceBase",System.getProperty("user.dir"))
    defaultServ.setInitParameter("dirAllowed","true")
    context.addServlet(defaultServ,"/")
    server.setHandler(context)

    logger.info(s"Starting jetty serving ${System.getProperty("user.dir")}")

    server.start()
  }

  override def afterAll() {
    logger.info(s"Stopping jetty server.")
    server.stop
  }

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

    assert(srcTypes.size == 5)
    assert(srcTypes.count(_.name == "balancedelta2017") == 1)
    assert(srcTypes.count(_.topic == imbTopic) == 1)
  }

  test("TennetSourceRecordProducer") {
    val mock = new MockOffsetStorageReader
    val producer = new TennetSourcePoller(TestData.connectConfiguration, mock)
    val records = producer.getRecords
    assert(records.size==30)
  }

  test("TennetXML offset") {
    object mockServiceProvider extends ServiceProvider {
      override val xmlReader = HttpXmlReader
      override val storageReader = new MockOffsetStorageReader()
      override val clock = Clock.systemUTC()
    }

    lazy val tennetXml = BalanceDeltaSourceRecordProducer(mockServiceProvider, TestData.balanceDeltaSourceType)
    val records: Seq[SourceRecord] = tennetXml.produce
    assert(records.size==30)
  }

  test("Test embedded server") {
    val req = Http("http://localhost:8899/testdata/balancedelta2017/BALANCE_DELTA.xml")
    val resp = req.asString.body
    println(resp)
  }
//  test("Test datelist") {
//       val dayList = TennetHelper.createPrevDaysList(4)
//       dayList should contain (LocalDate.now.plusDays(-4))
//       dayList should contain (LocalDate.now.plusDays(-1))
//       assert(dayList.size==4)
//  }
}