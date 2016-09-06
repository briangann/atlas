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
import akka.actor.ActorRef
import akka.actor.ActorSystem

import akka.cluster

import akka.persistence._
import akka.actor.ActorLogging
import com.netflix.atlas.core.db.Database

import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}

import com.netflix.atlas.json.Json
import com.netflix.atlas.core.model._

import com.netflix.atlas.webapi.GraphApi._
import com.netflix.atlas.webapi.TagsApi._


import akka.cluster.sharding.{ClusterShardingSettings, ClusterSharding}

import java.math.BigInteger

import org.slf4j.LoggerFactory

object ClusteredDatabaseActor{
  import com.netflix.atlas.webapi.PublishApi._
  
  private val logger = LoggerFactory.getLogger(getClass)

  def shardName = "ClusteredDatabaseActor"

  case class GetShardedData(shardId: Int, req: DataRequest)
  case class GetShardedTags(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery)
  case class GetShardedTagValues(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery)
  case class GetShardedTagKeys(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery)
  val extractShardId: ShardRegion.ExtractShardId = msg => msg match {
    case GetShardedData(shardId: Int, req: DataRequest) =>
      //logger.info("************** GetShardedData explicit shardid = " + shardId)
      BigInteger.valueOf(shardId).toString()     
    case GetShardedTags(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery) =>
      //logger.info("************** GetShardedTags explicit shardid = " + shardId)
      BigInteger.valueOf(shardId).toString()     
    case GetShardedTagValues(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery) =>
      //logger.info("************** GetShardedTagValues explicit shardid = " + shardId)
      BigInteger.valueOf(shardId).toString()     
    case GetShardedTagKeys(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery) =>
      //logger.info("************** GetShardedTagKeys explicit shardid = " + shardId)
      BigInteger.valueOf(shardId).toString()      
  }
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case d: GetShardedData =>  (d.shardId.toString(), d)
    case d: GetShardedTags =>  (d.shardId.toString(), d)
    case d: GetShardedTagKeys =>  (d.shardId.toString(), d)
    case d: GetShardedTagValues =>  (d.shardId.toString(), d)
  }
}

case object ShutdownClusteredDatabase

case class ClusterDatabaseCmd(data: String)
case class ClusterDatabaseEvt(data: String)

case class ClusterDatabaseState(events: List[String] = Nil) {
  def updated(evt: ClusterDatabaseEvt): ClusterDatabaseState = copy(evt.data :: events)
  def size: Int = events.length
  override def toString: String = events.reverse.toString
}


class ClusteredDatabaseActor(db: Database,  implicit val system: ActorSystem) extends PersistentActor with ActorLogging {
  import ClusteredDatabaseActor._
  import akka.cluster._
  import akka.cluster.ClusterEvent._
  import com.netflix.atlas.webapi.GraphApi._
  import com.netflix.atlas.webapi.TagsApi._
  import scala.concurrent.duration._

  override def persistenceId = self.path.parent.name + "-" + self.path.name
  // passivate the entity when there is no activity
  //context.setReceiveTimeout(2.minutes)
 
  var state = ClusterDatabaseState()
 
  def updateState(event: ClusterDatabaseEvt): Unit =
    state = state.updated(event)
 
  def numEvents =
    state.size
    
  val receiveRecover: Receive = {
    case evt: ClusterDatabaseEvt =>
      //updateState(evt)
      // do nothing
    case SnapshotOffer(_, snapshot: ClusterDatabaseState) => state = snapshot
  }
  
  val receiveCommand: Receive = {
    case ClusterDatabaseCmd(data) =>
      // disabled
      //persist(ClusterDatabaseEvt(s"${data}-${numEvents}"))(updateState)
      //persist(ClusterDatabaseEvt(s"${data}-${numEvents + 1}")) { event =>
      //  updateState(event)
      //  context.system.eventStream.publish(event)
      //}
    case "snap"  => saveSnapshot(state)
    case ShutdownClusteredDatabase =>
      context.stop(self)
    case ListTagsRequest(tq)    =>
      log.debug("ClusteredDatabaseActor received list tags request")
      sender() ! TagListResponse(db.index.findTags(tq))
    case ListKeysRequest(tq)    => sender() ! KeyListResponse(db.index.findKeys(tq).map(_.name))
    case ListValuesRequest(tq)  => sender() ! ValueListResponse(db.index.findValues(tq))
    case GetShardedData(shardId: Int, req: DataRequest) =>
       //log.debug("GetShardedData sending back OK")
       var resp = executeDataRequest(req)
       //log.debug("GetShardedData resp is " + resp)
       sender() ! resp
    case GetShardedTags(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery) =>
      var resp = TagListResponse(db.index.findTags(tq))
       //log.debug("GetShardedTags resp is " + resp)
       sender() ! resp
    case GetShardedTagKeys(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery) =>
      var resp = KeyListResponse(db.index.findKeys(tq).map(_.name))
       //log.debug("GetShardedTags resp is " + resp)
       sender() ! resp
    case GetShardedTagValues(shardId: Int, tq: com.netflix.atlas.core.index.TagQuery) =>
      var resp = ValueListResponse(db.index.findValues(tq))
       //log.debug("GetShardedTagValues resp is " + resp)
       sender() ! resp
  }
  
  
  private def executeDataRequest(req: DataRequest): DataResponse = {
    val data = req.exprs.map(expr => expr -> db.execute(req.context, expr)).toMap
    DataResponse(data)
  }
}

