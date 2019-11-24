// copy from
// http://mkuthan.github.io/blog/2016/01/29/spark-kafka-integration2/

package com.sparkkafka.uber

import java.util.concurrent.atomic.AtomicReference

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class RDDExceptionHandler extends Callback{
  private val lastException = new AtomicReference[Option[Exception]](None)

  override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
    lastException.set(Option(e))
  }

  def throwExceptionIfAny(): Unit = {
    lastException.getAndSet(None).foreach(ex => throw ex)
  }
}
