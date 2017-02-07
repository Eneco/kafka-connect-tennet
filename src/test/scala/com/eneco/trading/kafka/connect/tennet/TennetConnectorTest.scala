package com.eneco.trading.kafka.connect.tennet

import java.util
import java.util.Collections

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.xml.NodeSeq
import scalaj.http._

import scala.collection.JavaConverters._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class TennetConnectorTest extends FunSuite with Matchers with BeforeAndAfter with MockitoSugar {
  test("testing get xml") {

    val partition: util.Map[String, String] = Collections.singletonMap("", "")
    //as a list to search for
    val partitionList: util.List[util.Map[String, String]] = List(partition).asJava
    //set up the offset
    val offset: util.Map[String, Object] = Collections.singletonMap("", "")
    //create offsets to initialize from
    val offsets :util.Map[util.Map[String, String],util.Map[String, Object]] = Map(partition -> offset).asJava

    //mock out reader and task context
    val taskContext = mock[SourceTaskContext]
    val reader = mock[OffsetStorageReader]
    when(reader.offsets(partitionList)).thenReturn(offsets)
    when(taskContext.offsetStorageReader()).thenReturn(reader)

    val response: HttpResponse[String] = Http("http://www.tennet.org/xml/balancedeltaprices/balans-delta_2h.xml").asString
    val x = TennetXml(taskContext.offsetStorageReader(), response.body)
    val records: Seq[ImbalanceRecord] = x.fromBody()
    (records.size > 0) shouldBe true
  }
 }



