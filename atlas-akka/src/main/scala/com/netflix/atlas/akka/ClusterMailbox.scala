/*
 * Copyright 2014-2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.akka

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.dispatch.Envelope
import akka.dispatch.MailboxType
import akka.dispatch.MessageQueue
import akka.dispatch.ProducesMessageQueue
import akka.dispatch.DequeBasedMessageQueueSemantics
import com.netflix.spectator.api.Spectator
import com.typesafe.config.Config


object ClusterMailbox {

  private case class Entry(v: Envelope, t: Long = System.nanoTime)

  class MeteredMessageQueue(path: String) extends MessageQueue
      with DequeBasedMessageQueueSemantics {

    private final val queue = new ConcurrentLinkedQueue[Entry]

    private val registry = Spectator.globalRegistry()
    private val insertCounter = registry.counter("akka.queue.insert", "path", path)
    private val waitTimer = registry.timer("akka.queue.wait", "path", path)
    registry.collectionSize(registry.createId("akka.queue.size", "path", path), queue)

    def enqueueFirst(receiver: ActorRef, handle: Envelope): Unit = {
      println(s"\n\n*******************ClusterMailbox trying to enqueueFirst*******************")
      println(handle.message.toString())
      insertCounter.increment()
      queue.offer(Entry(handle))
    }
    
    def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
      println(s"\n\n*******************ClusterMailbox enqueue*******************")
      println(handle.message.toString())
      insertCounter.increment()
      queue.offer(Entry(handle))
    }

    def dequeue(): Envelope = {
      println(s"\n\n*******************ClusterMailbox dequeue*******************")       
      val tmp = queue.poll()
      if (tmp == null) null else {
        val dur = System.nanoTime - tmp.t
        waitTimer.record(dur, TimeUnit.NANOSECONDS)
        tmp.v
      }
    }

    def numberOfMessages: Int = queue.size
    def hasMessages: Boolean = !queue.isEmpty
    def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
      println(s"\n\n*******************ClusterMailbox cleanup*******************")       
      queue.clear()
    }
  }
}

class ClusterMailbox(settings: ActorSystem.Settings, config: Config) extends MailboxType
    with ProducesMessageQueue[ClusterMailbox.MeteredMessageQueue] {

  import com.netflix.atlas.akka.ClusterMailbox._

  private val Path = config.getString("path-pattern").r

  /** Summarizes a path for use in a metric tag. */
  def tagValue(path: ActorPath): String = {
    path.toString match {
      case Path(v) => v
      case _       => "uncategorized"
    }
  }

  final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    println(s"\n\n*******************ClusterMailbox create*******************")       
    val path = owner.fold("unknown")(r => tagValue(r.path))
    println(s"\n\n*******************ClusterMailbox create path is ${path}*******************")    
    new MeteredMessageQueue(path)
  }
}
