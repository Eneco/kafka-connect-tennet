package com.eneco.trading.kafka.connect.tennet

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

class TennetSourceConnector extends  SourceConnector with StrictLogging {

  private var configProps : Option[util.Map[String, String]] = None

  override def taskClass(): Class[_ <: Task] = classOf[TennetSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info("Setting task configuration with $maxTasks workers.")
    configProps match {
      case Some(props) => (1 to maxTasks).map(_ => props).toList.asJava
      case None => throw new ConnectException ("TaskConfigs not properly initialised" )
    }
  }

  override def stop(): Unit = {
    logger.info("stop")
  }

  override def config() = TennetSourceConfig.config

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Start Tennet Connector with: ")
    logger.info(props.toString())
    configProps = Some(props)
    Try(new TennetSourceConfig(props)) match {
      case Failure(_) => throw new ConnectException("Couldn't start due to configuration error")
      case _ => logger.info("Started Tennet source connector")
    }
  }

  override def version(): String = "1.0.0"
}
