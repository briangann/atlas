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
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model._
import com.netflix.atlas.json.Json
import spray.can.Http
import spray.http.MediaTypes._
import spray.http._

// for sharding and queries
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.{ClusterShardingSettings, ClusterSharding}
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

class TagsRequestActor extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.TagsApi._
  import com.netflix.atlas.config.ConfigManager
  //com.netflix.atlas.core.index.TagQuery

  val dbRef = ClusterSharding(context.system).shardRegion(ClusteredDatabaseActor.shardName)
  private val config = ConfigManager.current.getConfig("atlas.akka.atlascluster")
  private val numberOfShards = config.getInt("number-of-shards")

  var request: Request = _
  var responseRef: ActorRef = _

  def receive = {
    case v => try innerReceive(v) catch DiagnosticMessage.handleException(sender())
  }

  def innerReceive: Receive = {
    case req: Request =>
      request = req
      responseRef = sender()
      var aDbRequest = req.toDbRequest
      var tq: com.netflix.atlas.core.index.TagQuery = null
      (req.key -> req.verbose) match {
        case (Some(k),  true) => tq = req.toTagQuery(Query.HasKey(k))
        case (Some(k), false) => tq = req.toTagQuery(Query.HasKey(k))
        case (None,     true) => tq = req.toTagQuery(Query.True)
        case (None,    false) => tq = req.toTagQuery(Query.True)
      }
      
      aDbRequest match {
        case x: ListTagsRequest =>
          var z = tq.query.get
          val pairs = Query.tags(z)
          var ti = TaggedItem.computeId(pairs)
          println("Sharded ListTagsRequest: Sending db req " + req.toDbRequest.toString())
          // ask all shards
          var shardId: Int = 0
          var results: List[TagListResponse] = List()
          for (shardId <- 0 to numberOfShards -1) {
            val future = dbRef.ask(ClusteredDatabaseActor.GetShardedTags(shardId, tq))(5.seconds)
            println("Sharded ListTagsRequest: waiting....")
            // TODO: Handle exceptions
            var aresult: TagListResponse = Await.result(future, 5.seconds).asInstanceOf[TagListResponse]
            println("Sharded ListTagsRequest: got something...")
            println("Sharded ListTagsRequest: handleReq future response is " + aresult)
            results = aresult :: results
          }
          println("Sharded ListValuesRequest: result as list is " + results)
          // merge results
          var data = List[Tag]() 
          results.foreach { aDataResponse =>
            // the response has a list of strings, iterate those and append to drData
            aDataResponse.vs.foreach { aValue =>
              data = aValue :: data
            }
            // reduce to distinct values
            data = data.distinct
          }
          
          var mergedData = TagListResponse(data)
          println("Sharded ListValuesRequest: merged data: " + mergedData)
          // send the result back to ourself, so we can re-use the non-clustered case statements
          self ! mergedData

        case x: ListValuesRequest =>
          var z = tq.query.get
          val pairs = Query.tags(z)
          var ti = TaggedItem.computeId(pairs)
          println("Sharded ListValuesRequest: Sending db req " + req.toDbRequest.toString())
          // ask all shards
          var shardId: Int = 0
          var results: List[ValueListResponse] = List()
          for (shardId <- 0 to numberOfShards -1) {
            val future = dbRef.ask(ClusteredDatabaseActor.GetShardedTagValues(shardId, tq))(5.seconds)
            println("Sharded ListValuesRequest: waiting....")
            // TODO: Handle exceptions
            var aresult: ValueListResponse = Await.result(future, 5.seconds).asInstanceOf[ValueListResponse]
            println("Sharded ListValuesRequest: got something...")
            println("Sharded ListValuesRequest: handleReq future response is " + aresult)
            results = aresult :: results
          }
          println("Sharded ListValuesRequest: result as list is " + results)
          // merge results
          var data = List[String]() 
          results.foreach { aDataResponse =>
            // the response has a list of strings, iterate those and append to drData
            aDataResponse.vs.foreach { aValue =>
              data = aValue :: data
            }
            // reduce to distinct values
            data = data.distinct
          }
          
          var mergedData = ValueListResponse(data)
          println("Sharded ListValuesRequest: merged data: " + mergedData)
          // send the result back to ourself, so we can re-use the non-clustered case statements
          self ! mergedData

        case x: ListKeysRequest =>
          var z = tq.query.get
          val pairs = Query.tags(z)
          var ti = TaggedItem.computeId(pairs)
          println("Sharded ListKeysRequest: Sending db req " + req.toDbRequest.toString())
          // ask all shards
          var shardId: Int = 0
          var results: List[KeyListResponse] = List()
          for (shardId <- 0 to numberOfShards -1) {
            val future = dbRef.ask(ClusteredDatabaseActor.GetShardedTagKeys(shardId, tq))(5.seconds)
            println("Sharded ListKeysRequest: waiting....")
            // TODO: Handle exceptions
            var aresult: KeyListResponse = Await.result(future, 5.seconds).asInstanceOf[KeyListResponse]
            println("Sharded ListKeysRequest: got something...")
            println("Sharded ListKeysRequest: handleReq future response is " + aresult)
            results = aresult :: results
          }
          println("Sharded ListKeysRequest: result as list is " + results)
          // merge results
          var data = List[String]() 
          results.foreach { aDataResponse =>
            // the response has a list of strings, iterate those and append to drData
            aDataResponse.vs.foreach { aValue =>
              data = aValue :: data
            }
            // reduce to distinct values
            data = data.distinct
          }
          
          var mergedData = KeyListResponse(data)
          println("Sharded ListKeysRequest: merged data: " + mergedData)
          // send the result back to ourself, so we can re-use the non-clustered case statements
          self ! mergedData

        case _ => println("error Unknown class")
      }
  
      
      //dbRef.tell(req.toDbRequest, self)
    case TagListResponse(vs)   if request.useText => sendText(tagString(vs), offsetTag(vs))
    case KeyListResponse(vs)   if request.useText => sendText(vs.mkString("\n"), offsetString(vs))
    case ValueListResponse(vs) if request.useText => sendText(vs.mkString("\n"), offsetString(vs))
    case TagListResponse(vs)   if request.useJson => sendJson(vs, offsetTag(vs))
    case KeyListResponse(vs)   if request.useJson => sendJson(vs, offsetString(vs))
    case ValueListResponse(vs) if request.useJson => sendJson(vs, offsetString(vs))
    case ev: Http.ConnectionClosed =>
      log.info("connection closed")
      context.stop(self)
  }

  private def tagString(t: Tag): String = s"${t.key}\t${t.value}\t${t.count}"

  private def tagString(ts: List[Tag]): String = ts.map(tagString).mkString("\n")

  private def offsetString(vs: List[String]): Option[String] = {
    if (vs.size < request.limit) None else Some(vs.last)
  }

  private def offsetTag(vs: List[Tag]): Option[String] = {
    if (vs.size < request.limit) None else {
      val t = vs.last
      Some(s"${t.key},${t.value}")
    }
  }

  private def sendJson(data: AnyRef, offset: Option[String]): Unit = {
    val entity = HttpEntity(`application/json`, Json.encode(data))
    val headers = offset.map(v => HttpHeaders.RawHeader(offsetHeader, v)).toList
    responseRef ! HttpResponse(StatusCodes.OK, entity, headers)
    context.stop(self)
  }

  private def sendText(data: String, offset: Option[String]): Unit = {
    val entity = HttpEntity(`text/plain`, data)
    val headers = offset.map(v => HttpHeaders.RawHeader(offsetHeader, v)).toList
    responseRef ! HttpResponse(StatusCodes.OK, entity, headers)
    context.stop(self)
  }
}

