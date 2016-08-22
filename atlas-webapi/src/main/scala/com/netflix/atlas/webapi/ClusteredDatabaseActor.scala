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
package com.netflix.atlas.webapi

import akka.actor.Actor
import akka.persistence._
import akka.actor.ActorLogging
import com.netflix.atlas.core.db.Database

import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}

// for clustered nodes, we use pub-sub
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}

object ClusteredDatabaseActor{
  import com.netflix.atlas.webapi.PublishApi._

  def name = "ClusteredDatabaseActor"

  def extractShardId: ExtractShardId = {
    case PublishRequest(_, _) =>
      this.toString
  }

  def extractEntityId: ExtractEntityId = {
    case msg @ PublishRequest(_, _) =>
      (this.toString, msg)
  }
  case class Publish(msg: String)
  case class Message(from: String, text: String)

}

case object ShutdownClusteredDatabase

case class ClusterDatabaseCmd(data: String)
case class ClusterDatabaseEvt(data: String)

case class ClusterDatabaseState(events: List[String] = Nil) {
  def updated(evt: ClusterDatabaseEvt): ClusterDatabaseState = copy(evt.data :: events)
  def size: Int = events.length
  override def toString: String = events.reverse.toString
}

class ClusteredDatabaseActor(db: Database) extends PersistentActor with ActorLogging {

  import com.netflix.atlas.webapi.GraphApi._
  import com.netflix.atlas.webapi.TagsApi._

  override def persistenceId = "clustered-database-actor-1"
 
  var state = ClusterDatabaseState()
 
  val mediator = DistributedPubSub(context.system).mediator
  // this should be a config item... and work per cluster
  val topic = "database"
  mediator ! Subscribe(topic, self)
  
  def updateState(event: ClusterDatabaseEvt): Unit =
    state = state.updated(event)
 
  def numEvents =
    state.size
 
  val receiveRecover: Receive = {
    case evt: ClusterDatabaseEvt                          => updateState(evt)
    case SnapshotOffer(_, snapshot: ClusterDatabaseState) => state = snapshot
  }
 
  val receiveCommand: Receive = {
    case ClusterDatabaseCmd(data) =>
      persist(ClusterDatabaseEvt(s"${data}-${numEvents}"))(updateState)
      persist(ClusterDatabaseEvt(s"${data}-${numEvents + 1}")) { event =>
        updateState(event)
        context.system.eventStream.publish(event)
      }
    case "snap"  => saveSnapshot(state)
    case ShutdownClusteredDatabase =>
      context.stop(self)
    case ListTagsRequest(tq)    =>
      println("ClusteredDatabaseActor received list tags request")
      sender() ! TagListResponse(db.index.findTags(tq))
    case ListKeysRequest(tq)    => sender() ! KeyListResponse(db.index.findKeys(tq).map(_.name))
    case ListValuesRequest(tq)  => sender() ! ValueListResponse(db.index.findValues(tq))
    case req: DataRequest       =>
      // received a data request from an api, need to ask all of our peers for data, then
      // respond with the merged dataset
      sender() ! executeDataRequest(req)
    case ClusteredDatabaseActor.Message(from, text) =>
      if (sender == self) {
        println(s"I sent this message, ignoring")
      }
      else {
        println(s"another database sent me a message " + text)
      }
  }
  
  // merge all data responses together
  // val c = a.union(b).distinct
  private def executeDataRequest(req: DataRequest): DataResponse = {
    val data = req.exprs.map(expr => expr -> db.execute(req.context, expr)).toMap
    DataResponse(data)
  }
}

