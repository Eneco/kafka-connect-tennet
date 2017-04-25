package com.eneco.trading.kafka.connect.tennet

import java.time.{Clock, Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.mutable

/**
  * Created by jhofman on 22/02/2017.
  */
class TestBase extends FunSuite with Matchers with BeforeAndAfterAll with StrictLogging {

  def EpochMillis(t: String) = ZonedDateTime.parse(t, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli

  implicit def DoubleToOptionalFloat64(d: Double) : Option[Double] = Some(d)

  class MockXmlReader extends XmlReader {
    var content : Option[String] = None
    var queries = new mutable.ArrayBuffer[String]()
    override def getXml(url: String): Option[String] = {
      queries.append(url)
      content
    }
  }

  class MockServiceProvider(klock: Clock) extends ServiceProvider {
    def this() = this(Clock.systemUTC())
    def this(time: String) = this(Clock.fixed(ZonedDateTime.parse(time, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant(), Clock.systemUTC().getZone))
    val mockXmlReader = new MockXmlReader
    override val xmlReader = mockXmlReader
    override val storageReader = new MockOffsetStorageReader()
    override val clock = klock
  }
}
