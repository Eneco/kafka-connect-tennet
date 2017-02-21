package com.eneco.trading.kafka.connect.tennet

import java.time.{Instant, ZoneId}

import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.connect.source.SourceRecord

import scala.xml.Node

trait SourceRecordProducer {
  def produce: Seq[SourceRecord]
  def sourceName: String
  def mapRecord(record: Node) : TennetSourceRecord

  val url: String
  val generatedAt = Instant.now.toEpochMilli
  def body = TennetHelper.getXml(url)
  def hash = DigestUtils.sha256Hex(body)
}

abstract class TennetSourceRecord

