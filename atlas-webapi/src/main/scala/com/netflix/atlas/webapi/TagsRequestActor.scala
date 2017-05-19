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
import akka.actor.ActorSystem

import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model._
import com.netflix.atlas.json.Json
import spray.can.Http
import spray.http.MediaTypes._
import spray.http._

// for sharding and queries
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.{ClusterShardingSettings, ClusterSharding}
import scala.concurrent._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.netflix.spectator.api.Registry


class TagsRequestActor(registry: Registry, system: ActorSystem) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.TagsApi._
  import com.netflix.atlas.config.ConfigManager
  //com.netflix.atlas.core.index.TagQuery
  import org.slf4j.LoggerFactory

  private val logger = LoggerFactory.getLogger(getClass)

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
          logger.debug("Sharded ListTagsRequest: Sending db req " + req.toDbRequest.toString())
          // ask all shards
          var results: List[TagListResponse] = List()
          val shardList = List.range(0,numberOfShards)
          val futureMap = shardList.map {
            shardId =>
              logger.debug("Sharded ListTagsRequest: asking shard: " + shardId)
              val aFuture = dbRef.ask(ClusteredDatabaseActor.GetShardedTags(shardId, tq))(10.seconds).mapTo[TagListResponse]
              aFuture
          }
          val futureSequence = Future.sequence(futureMap)
          // master future to pull together all shard data
          val master = futureSequence.map { 
            responses =>
              responses.foreach { aResponse => 
                results = aResponse :: results
              }
          }
          master onComplete {
            case l => {
              // merge results
              var data = List[Tag]()
              logger.debug("Sharded ListTagsRequest: result size: " + results.length)

              results.foreach {
                aDataResponse =>
                  logger.debug("Sharded ListTagsRequest: aDataResponse: " + aDataResponse.toString())
                  // the response has a list of strings, iterate those and append to drData
                  aDataResponse.vs.foreach {
                    aValue =>
                      logger.debug("Sharded ListTagsRequest: aValue: " + aValue.toString())
                      data = aValue :: data
                  }
                  // reduce to distinct values
                  data = data.distinct
              }
              var mergedData = TagListResponse(data)
              logger.debug("Sharded ListTagsRequest.master.onComplete: All done, merged data is: " + mergedData + " dbReq was: " + req.toDbRequest.toString())
              // send the result back to ourself, so we can re-use the non-clustered case statements
              self ! mergedData
            }
          }
          // best effort - if there's a failure on an ask, discard it
          master onFailure {
            case l => {
              logger.warn("Sharded ListTagsRequest.master.onFailure: " + l)
            }
          }
        case x: ListValuesRequest =>
          var z = tq.query.get
          val pairs = Query.tags(z)
          var ti = TaggedItem.computeId(pairs)
          logger.debug("Sharded ListValuesRequest: Sending db req " + req.toDbRequest.toString())
          // ask all shards
          var results: List[ValueListResponse] = List()
          val shardList = List.range(0,numberOfShards)
          val futureMap = shardList.map {
            shardId =>
              logger.debug("Sharded ListValuesRequest: asking shard: " + shardId)
              val aFuture = dbRef.ask(ClusteredDatabaseActor.GetShardedTagValues(shardId, tq))(10.seconds).mapTo[ValueListResponse]
              aFuture
          }
          val futureSequence = Future.sequence(futureMap)
          // this is a new future that will pull together all of the shard requests
          // this is nonblocking since it is itself a future
          val master = futureSequence.map { 
            responses =>
              responses.foreach { aResponse => 
                results = aResponse :: results
              }
          }
          master onComplete {
            case l => {
              // merge results
              var data = List[String]()
              logger.debug("Sharded ListValuesRequest: result size: " + results.length)

              results.foreach {
                aDataResponse =>
                  logger.debug("Sharded ListValuesRequest: aDataResponse: " + aDataResponse.toString())

                  // the response has a list of strings, iterate those and append to drData
                  aDataResponse.vs.foreach {

                    aValue =>
                      logger.debug("Sharded ListValuesRequest: aValue: " + aValue.toString())
                      data = aValue :: data
                  }
                  // reduce to distinct values
                  data = data.distinct
              }
              var mergedData = ValueListResponse(data)
              logger.debug("Sharded ListValuesRequest.master.onComplete: All done, merged data is: " + mergedData + " dbReq was: " + req.toDbRequest.toString())

              // send the result back to ourself, so we can re-use the non-clustered case statements
              self ! mergedData
            }
          }
          // best effort - if there's a failure on an ask, discard it
          master onFailure {
            case l => {
              logger.warn("Sharded ListValuesRequest.master.onFailure: " + l)
            }
          }

        case x: ListKeysRequest =>
          var z = tq.query.get
          val pairs = Query.tags(z)
          var ti = TaggedItem.computeId(pairs)
          logger.debug("Sharded ListKeysRequest: Sending db req " + req.toDbRequest.toString())
          // ask all shards
          var results: List[KeyListResponse] = List()
          val shardList = List.range(0,numberOfShards)
          val futureMap = shardList.map {
            shardId =>
              logger.debug("Sharded ListKeysRequest: asking shard: " + shardId)
              val aFuture = dbRef.ask(ClusteredDatabaseActor.GetShardedTagKeys(shardId, tq))(10.seconds).mapTo[KeyListResponse]
              aFuture
          }
          val futureSequence = Future.sequence(futureMap)
          // master future to pull together all shard data
          val master = futureSequence.map { 
            responses =>
              responses.foreach { aResponse => 
                results = aResponse :: results
              }
          }
          master onComplete {
            case l => {
              // merge results
              var data = List[String]()
              logger.debug("Sharded ListKeysRequest: result size: " + results.length)
              results.foreach {
                aDataResponse =>
                  // the response has a list of strings, iterate those and append to drData
                  logger.debug("Sharded ListKeysRequest: aDataResponse: " + aDataResponse.toString())
                  aDataResponse.vs.foreach {
                    aValue =>
                      logger.debug("Sharded ListKeysRequest: aValue: " + aValue.toString())
                      data = aValue :: data
                  }
                  // reduce to distinct values
                  data = data.distinct
              }
              var mergedData = KeyListResponse(data)
              logger.debug("Sharded ListKeysRequest.master.onComplete: All done, merged data is: " + mergedData + " dbReq was: " + req.toDbRequest.toString())
              // send the result back to ourself, so we can re-use the non-clustered case statements
              self ! mergedData
            }
          }
          // best effort - if there's a failure on an ask, discard it
          master onFailure {
            case l => {
              logger.warn("Sharded ListKeysRequest.master.onFailure: " + l)
            }
          }
          
        case _ => logger.error("error Unknown class")
      }
  
      
      //dbRef.tell(req.toDbRequest, self)
    case TagListResponse(vs)   if request.useText => sendText(tagString(vs), offsetTag(vs))
    case KeyListResponse(vs)   if request.useText => sendText(vs.mkString("\n"), offsetString(vs))
    case ValueListResponse(vs) if request.useText => sendText(vs.mkString("\n"), offsetString(vs))
    case TagListResponse(vs)   if request.useJson => sendJson(vs, offsetTag(vs))
    case KeyListResponse(vs)   if request.useJson => sendJson(vs, offsetString(vs))
    case ValueListResponse(vs) if request.useJson => sendJson(vs, offsetString(vs))
    case ev: Http.ConnectionClosed =>
      logger.debug("connection closed")
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

