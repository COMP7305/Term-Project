// copy from
// http://mkuthan.github.io/blog/2016/01/29/spark-kafka-integration2/

package com.sparkkafka.uber

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory
import java.util.logging.Logger
import org.apache.spark.streaming.dstream.DStream

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients

object RDDImprovement{
  import scala.language.implicitConversions

  implicit def RddToKafka(rdd: RDD[String]) : RDDImprovement ={
    new RDDImprovement(rdd)
  }

  class RDDImprovement(rdd:RDD[String]){
    def sendToKafka(config: Map[String, String], topic: String): Unit = {
      rdd.foreachPartition{ records=>
        val producer = KafkaProducerFactory.getOrCreateProducer(config)
        val context = TaskContext.get
        val logger = LoggerFactory.getLogger(classOf[RDDImprovement])
        val callback = new RDDExceptionHandler

        logger.debug(s"Send Spark partition: ${context.partitionId} to Kafka topic: $topic")
        val metadata = records.map { record =>
          callback.throwExceptionIfAny()
          val producerRecord = new ProducerRecord[String, String](topic, record)
          producer.send(producerRecord, callback)
        }.toList

        logger.debug(s"Flush Spark partition: ${context.partitionId} to Kafka topic: $topic")
        metadata.foreach { println }
      }
    }
  }
}


