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

import akka.persistence._
import akka.actor.ActorLogging
import com.netflix.atlas.core.db.Database

import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}

import com.netflix.atlas.json.Json
import com.netflix.atlas.core.model._

import com.netflix.atlas.webapi.GraphApi._
import com.netflix.atlas.webapi.TagsApi._


object ClusteredDatabaseActor{
  import com.netflix.atlas.webapi.PublishApi._

  def shardName = "ClusteredDatabaseActor"

  val numberOfShards = 2
 
  //case class GetData(req: DataRequest, actorRef: ActorRef)
  case class GetData(req: com.netflix.atlas.webapi.GraphApi.Request, actorRef: ActorRef)
  
  val extractShardId: ShardRegion.ExtractShardId = msg => msg match {
    case GetData(req, actorRef: ActorRef) =>
      println("************** extract shardid with GetData *********")
      (math.abs(req.hashCode) % numberOfShards).toString
    case req: DataRequest =>
      println("************** extract shardid with req datarequest *********")
      (math.abs(req.hashCode) % numberOfShards).toString
  }
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case d: GetData =>  (shardName, d)
  }
      
  case class Publish(msg: String, req: com.netflix.atlas.webapi.GraphApi.DataRequest)
  // the DataResponse is not directly serializable, so just json encode it...
  case class Message(msg: String, req: com.netflix.atlas.webapi.GraphApi.DataRequest, response: String)

}

case object ShutdownClusteredDatabase

case class ClusterDatabaseCmd(data: String)
case class ClusterDatabaseEvt(data: String)

case class ClusterDatabaseState(events: List[String] = Nil) {
  def updated(evt: ClusterDatabaseEvt): ClusterDatabaseState = copy(evt.data :: events)
  def size: Int = events.length
  override def toString: String = events.reverse.toString
}

//class GetData(req: DataRequest, actorRef: ActorRef)

class ClusteredDatabaseActor(db: Database) extends PersistentActor with ActorLogging {
  import ClusteredDatabaseActor._
  import ShardRegion.Passivate
  import com.netflix.atlas.webapi.GraphApi._
  import com.netflix.atlas.webapi.TagsApi._
  import scala.concurrent.duration._

  override def persistenceId = self.path.parent.name + "-" + self.path.name
  // passivate the entity when no activity
  //context.setReceiveTimeout(2.minutes)
 
  var state = ClusterDatabaseState()
 
  def updateState(event: ClusterDatabaseEvt): Unit =
    state = state.updated(event)
 
  def numEvents =
    state.size
    
  // the sender is stored here, and will be used once the data has been aggregated
  // from all peers
  var senderRef: ActorRef = _
  var mergedMap: Map[DataExpr, List[TimeSeries]] = null
  //var mergedMap: DataResponse = null
  var peerCount = 1
  var peerResponses = 0
  var partitionEnabled: Boolean = true
 
  val receiveRecover: Receive = {
    case evt: ClusterDatabaseEvt                          => updateState(evt)
    case SnapshotOffer(_, snapshot: ClusterDatabaseState) => state = snapshot
  }
 
  //def receiveCommand = {
  //   case GetData(req) => sender() ! executeDataRequest(req)
  //}
  
  
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
    case GetData(req,actorRef) => 
      println("CDA - GetData called")
      actorRef ! executeDataRequest(req.toDbRequest)
    case req: DataRequest => 
      println("CDA - req: DataRequest called")
      sender() ! executeDataRequest(req)
    /*
    case req: DataRequest       =>
      // received a data request from an api, need to ask all of our peers for data, then
      // respond with the merged dataset
      //
      // check if we are partitioned... if not, just do the request
      if (partitionEnabled) {
        // store our sender
        senderRef = sender()
        // ask our peers for their data
        //mediator ! Publish(topic,ClusteredDatabaseActor.Message("database-req", Json.encode(req)))
        senderRef ! GetTimeSeries(req)
        mediator ! Publish(topic,ClusteredDatabaseActor.Message("database-req", req, null))
      }
      else {
        sender() ! executeDataRequest(req)
      }
      
      */
    
    /*
    case ClusteredDatabaseActor.Message(messageType, req, response) =>
      if (messageType == "database-req") {
        if (sender == self) {
          println(s"I sent this message, will process my query now")
          var myresponse = executeDataRequest(req)
          println(s"Finished my DR, here's my ts response: " + myresponse.toString());
          mediator ! Publish("database",ClusteredDatabaseActor.Message("database-req-response", null, meh))
        }
        else {
          //println(s"another database sent me a message " + text)
          //var req = Json.decode[DataRequest](text)
          var myresponse = executeDataRequest(req)
          println(s"Finished my DR, here's my response: " + myresponse.toString());
          // DataResponse isn't directly deserializable,  break it up into two pieces that can be reassembled
          // now publish back our response
          mediator ! Publish("database",ClusteredDatabaseActor.Message("database-req-response", null, Json.encode(myresponse)))
        }
      }
      if (messageType == "database-req-response") {
        // decode
        peerResponses += 1
        println(s"database-req-response num peer responses: ${peerResponses}")
        println(s"peer response: " + response)
        var aTimeSeriesList = Json.decode[List[TimeSeries]](response)
        
        println(s"peer ts list: " + aTimeSeriesList.toString())
        
        // now do our datarequest (yes this is a duplicate for now)
        var myresponse = executeDataRequest(req)
        println("myresponse data before: " + Json.encode(myresponse.ts.values.toList))
        // then merge the peer responses into it
        // union and distinct will dedupe us
        //val c = aTimeSeriesList.union(myresponse.ts.values.toList).distinct
        val c = aTimeSeriesList.union(myresponse.ts.values.toList)
        
        // make a whole new data response
        val data = req.exprs.map(expr => expr -> c).toMap

                
        println("data after: " + Json.encode(data))
        // need all peers to respond
        if (peerResponses == peerCount) {
          println(s"database-req-response all peers responded")
          // have all peer responses merged, send the result back to
          // the original sender
          var fullResponse: DataResponse = new DataResponse(mergedMap)
          println("full response: " + Json.encode(mergedMap))
          senderRef ! fullResponse
          peerResponses = 0
          mergedMap = null
        }
      }
      
      */
  }
  
  
  // merge all data responses together
  // val c = a.union(b).distinct
  private def executeDataRequest(req: DataRequest): DataResponse = {
    val data = req.exprs.map(expr => expr -> db.execute(req.context, expr)).toMap
    DataResponse(data)
  }
}

