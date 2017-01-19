package com.eneco.trading.kafka.connect.tennet

import java.util
import java.util.Calendar

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scalaj.http.{Http, HttpResponse}
import scala.collection.JavaConverters._

case class TennetSourceRecordProducer(offsetStorageReader: OffsetStorageReader) {

  var partition = Calendar.getInstance().get(Calendar.DAY_OF_YEAR)

  def produce(topic: String) : Seq[SourceRecord] = {
    val response : HttpResponse[String] = Http("http://www.tennet.org/xml/balancedeltaprices/balans-delta_2h.xml").asString
    val imbalance = scala.xml.XML.loadString(response.body)
    val records = (imbalance \\ "RECORD").map(record =>
      ImbalanceRecord (
        (record \ "NUMBER").text.toInt,
        (record \ "SEQUENCE_NUMBER").text.toInt ,
        (record \ "TIME").text,
        (record \ "UPWARD_DISPATCH").text.toDouble,
        (record \ "DOWNWARD_DISPATCH").text.toDouble,
        (record \ "RESERVE_UPWARD_DISPATCH").text.toDouble,
        (record \ "RESERVE_DOWNWARD_DISPATCH").text.toDouble,
        (record \ "EMERGENCY_POWER").text.toDouble)
      )

    records.map(r =>
      new SourceRecord(
        ConnectPartition(), //source partitions?
        ConnectOffset(), //source offsets?
        topic,
        ImbalanceSourceRecord.schema,
        ImbalanceSourceRecord.struct(r))
      )
 }

  def ConnectPartition(): util.Map[String, Int] = {
    Map("partition" -> partition).asJava
  }

  def ConnectOffset(): util.Map[String, Long] = {
        Map("timestamp" -> System.currentTimeMillis
        ).asJava
      }

  def RecordPayload: Int = {
    Calendar.getInstance().get(Calendar.SECOND)
  }
}

abstract class Record
case class ImbalanceRecord(
                             Number : Int,
                             SequenceNumber : Int,
                             Time : String,
                             UpwardDispatch : Double,
                             DownwardDispatch : Double,
                             ReserveUpwardDispatch : Double,
                             ReserverDownwardDispatch : Double,
                             EmergencyPower : Double
                           ) extends Record

object ImbalanceSourceRecord {
  def struct(io: ImbalanceRecord) =
    new Struct(schema)
      .put("number", io.Number)
      .put("sequence_number", io.SequenceNumber)
      .put("time",io.Time)
      .put("upward_dispatch", io.UpwardDispatch)
      .put("downward_dispatch", io.DownwardDispatch)
      .put("reserve_upward_dispatch", io.ReserveUpwardDispatch)
      .put("reserve_downward_dispatch", io.ReserverDownwardDispatch)
      .put("emergency_power", io.EmergencyPower)

  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.tennet")
    .field("number", Schema.INT32_SCHEMA)
    .field("sequence_number", Schema.INT32_SCHEMA)
    .field("time", Schema.STRING_SCHEMA)
    .field("upward_dispatch", Schema.FLOAT64_SCHEMA)
    .field("downward_dispatch", Schema.FLOAT64_SCHEMA)
    .field("reserve_upward_dispatch", Schema.FLOAT64_SCHEMA)
    .field("reserve_downward_dispatch", Schema.FLOAT64_SCHEMA)
    .field("emergency_power",Schema.FLOAT64_SCHEMA)
    .build()
}
