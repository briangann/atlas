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

import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.config.ConfigManager
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.Interner
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.core.validation.Rule
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import spray.routing.RequestContext


import com.netflix.atlas.core.model._

import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.{ClusterShardingSettings, ClusterSharding}
import scala.concurrent._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

import spray.http.HttpEntity
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.http.StatusCode
import spray.http.StatusCodes

import java.math.BigInteger


import scala.concurrent.ExecutionContext.Implicits.global


class PublishApi(implicit val actorRefFactory: ActorRefFactory, implicit val system: ActorSystem) extends WebApi {

  import com.netflix.atlas.webapi.PublishApi._

  import org.slf4j.LoggerFactory
  private val logger = LoggerFactory.getLogger(getClass)
  //private val publishRef = actorRefFactory.actorSelection("/user/publish")
  val publishRef = ClusterSharding(system).shardRegion(ClusteredPublishActor.shardName)

  private val config = ConfigManager.current.getConfig("atlas.webapi.publish")

  private val internWhileParsing = config.getBoolean("intern-while-parsing")
  // shard config
  private val clusterConfig = ConfigManager.current.getConfig("atlas.akka.atlascluster")
  private val numberOfShards = clusterConfig.getInt("number-of-shards")
  private val rules = ApiSettings.validationRules

  def routes: RequestContext => Unit = {
    post {
      path("api" / "v1" / "publish") { ctx =>
        handleReq(ctx)
      } ~
      path("api" / "v1" / "publish-fast") { ctx =>
        // Legacy path from when there was more than one publish mode
        handleReq(ctx)
      }
    }
  }

  private def handleReq(ctx: RequestContext): Unit = {
    try {
      getJsonParser(ctx.request) match {
        case Some(parser) =>
          // TODO see side effect of internwhileparsing
          val data = decodeBatch(parser, internWhileParsing)
          // build a new req for each datapoint so we can have the taggeditem ride along
          // that is unique to the ingestion
          //
          // This uses a "future" to ingest everything and get the response
          var ingestionStatusCode: StatusCode = StatusCodes.OK
         
          // the ingestion process is a sequence of futures that only completes when the children are finished, giving the
          // "worst" result of the ingestion process as the response
          var futureMap = data.map {
            x =>
              // start with computing the ID based on all tags
              var ti = TaggedItem.computeId(x.tags)
              // workaround...
              // the shardid is computed from the name of the metric only, this forces a metric to always end up on the same shard
              x.tags.foreach {
                aTag =>
                  if (aTag._1 == "name") {
                    // found name, get its value
                    var aTagName = aTag._2
                    // create a new Map
                    var justName = Map[String,String](aTag._1  -> aTag._2 )
                    // now compute the shard id
                    ti = TaggedItem.computeId(justName)
                    ti = ti.abs().mod(BigInteger.valueOf(numberOfShards))
                  }
              }
              // alternative method is to compute from all of the tags, which spreads the time series across shards, but retrieval/merging becomes
              // problematic - use simple method for now
              // var ti = TaggedItem.computeId(x.tags)
              //var id = x.idString
              
              logger.debug("PublishApi.Ingest: taggedItem ID: " + ti)
              var newList: List[Datapoint] = List(x)
              val aReq = validate(newList)
              logger.debug(s"PublishApi.Ingest future will send request to " + publishRef.toString())
              // use a future to send the data
              val aFuture = publishRef.ask(ClusteredPublishActor.IngestTaggedItem(ti, aReq))(5.seconds).mapTo[HttpResponse]
              aFuture
          }
          val futureSequence = Future.sequence(futureMap)
          
          // collect the responses
          val master = futureSequence.map { 
            responses =>
              responses.foreach { aResponse => 
                // determine what response we should send back
                aResponse.status match {
                  case StatusCodes.BadRequest =>
                    logger.warn("PublishApi.Ingest.master.onSuccess: request was bad")
                    ingestionStatusCode = aResponse.status
                  case StatusCodes.OK =>
                    //logger.info("PublishApi.Ingest.master.onSuccess: all metrics ingested")
                    ingestionStatusCode = aResponse.status
                  case StatusCodes.Accepted =>
                    logger.warn("PublishApi.Ingest.master.onSuccess: some metrics rejected")
                    ingestionStatusCode = aResponse.status
                  case _ =>
                    logger.warn("PublishApi.Ingest.master.onSuccess: unrecognized response from publishactor: " + aResponse)
                    ingestionStatusCode = StatusCodes.BadRequest
                }
            }
          }
          master onComplete {
            case l => { 
              //logger.info("PublishApi.Ingest.master.onComplete entered")
              var aResponse = HttpResponse(ingestionStatusCode)
              //logger.info("PublishApi.Ingest.master.onComplete: onComplete response is " + aResponse)
              // now send the response, the futures are done
              //logger.info("PublishApi.Ingest.master.onComplete: Sending response..." + ingestionStatusCode)
              ctx.responder ! HttpResponse(ingestionStatusCode)
            }
          }
          // best effort - if there's a failure on an ask, discard it
          master onFailure {
           case aFailure => {
              logger.debug("PublishApi.Ingest.master.onFailure: " + aFailure)
              // queue this request and try again, let the sender know we'll process later
              ctx.responder ! HttpResponse(StatusCodes.Processing)
              // ok lets try again
              //logger.debug("PublishApi.Ingest.master.onFailure: Requeue")
              handleReq(ctx)
            }
          }
        case None =>
          throw new IllegalArgumentException("empty request body")
      }
    } catch handleException(ctx)
  }

  private def validate(vs: List[Datapoint]): PublishRequest = {
    val validDatapoints = List.newBuilder[Datapoint]
    val failures = List.newBuilder[ValidationResult]
    val now = System.currentTimeMillis()
    val limit = ApiSettings.maxDatapointAge
    vs.foreach { v =>
      val diff = now - v.timestamp
      val result = diff match {
        case d if d > limit =>
          val msg = s"data is too old: now = $now, timestamp = ${v.timestamp}, $d > $limit"
          ValidationResult.Fail("DataTooOld", msg)
        case d if d < -limit =>
          val msg = s"data is from future: now = $now, timestamp = ${v.timestamp}"
          ValidationResult.Fail("DataFromFuture", msg)
        case _ =>
          Rule.validate(v.tags, rules)
      }
      if (result.isSuccess) validDatapoints += v else failures += result
    }
    PublishRequest(validDatapoints.result(), failures.result())
  }
}

object PublishApi {

  import com.netflix.atlas.json.JsonParserHelper._

  type TagMap = Map[String, String]

  private final val maxPermittedTags = 30

  private def decodeTags(parser: JsonParser, commonTags: TagMap, intern: Boolean): TagMap = {
    val strInterner = Interner.forStrings
    val b = new SmallHashMap.Builder[String, String](2 * maxPermittedTags)
    if (commonTags != null) b.addAll(commonTags)
    foreachField(parser) { case key =>
      val value = parser.nextTextValue()
      if (value == null || value.isEmpty) {
        val loc = parser.getCurrentLocation
        val line = loc.getLineNr
        val col = loc.getColumnNr
        val msg = s"tag value cannot be null or empty (key=$key, line=$line, col=$col)"
        throw new IllegalArgumentException(msg)
      }
      if (intern)
        b.add(strInterner.intern(key), strInterner.intern(value))
      else
        b.add(key, value)
    }
    if (intern) TaggedItem.internTagsShallow(b.compact) else b.result
  }

  private def getValue(parser: JsonParser): Double = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.nextToken() match {
      case START_ARRAY        => nextDouble(parser)
      case VALUE_NUMBER_FLOAT => parser.getValueAsDouble()
      case VALUE_STRING       => java.lang.Double.valueOf(parser.getText())
      case t                  => fail(parser, s"expected VALUE_NUMBER_FLOAT but received $t")
    }
  }

  private def decode(parser: JsonParser, commonTags: TagMap, intern: Boolean): Datapoint = {
    var tags: TagMap = null
    var timestamp: Long = -1L
    var value: Double = Double.NaN
    foreachField(parser) {
      case "tags"      => tags = decodeTags(parser, commonTags, intern)
      case "timestamp" => timestamp = nextLong(parser)
      case "value"     => value = nextDouble(parser)
      case "start"     => timestamp = nextLong(parser) // Legacy support
      case "values"    => value = getValue(parser)
      case _           => // Ignore unknown fields
    }
    Datapoint(tags, timestamp, value)
  }

  def decodeDatapoint(parser: JsonParser, intern: Boolean = false): Datapoint = {
    decode(parser, null, intern)
  }

  def decodeDatapoint(json: String): Datapoint = {
    val parser = Json.newJsonParser(json)
    try decodeDatapoint(parser, false) finally parser.close()
  }

  def decodeBatch(parser: JsonParser, intern: Boolean = false): List[Datapoint] = {
    var tags: Map[String, String] = null
    var metrics: List[Datapoint] = null
    var tagsLoadedFirst = false
    foreachField(parser) {
      case "tags"    => tags = decodeTags(parser, null, intern)
      case "metrics" =>
        tagsLoadedFirst = (tags != null)
        val builder = List.newBuilder[Datapoint]
        foreachItem(parser) { builder += decode(parser, tags, intern) }
        metrics = builder.result
    }

    // If the tags were loaded first they got merged with the datapoints while parsing. Otherwise
    // they need to be merged here.
    if (tagsLoadedFirst || tags == null) {
      if (metrics == null) Nil else metrics
    } else {
      metrics.map(d => d.copy(tags = d.tags ++ tags))
    }
  }

  def decodeBatch(json: String): List[Datapoint] = {
    val parser = Json.newJsonParser(json)
    try decodeBatch(parser, false) finally parser.close()
  }

  def decodeList(parser: JsonParser, intern: Boolean = false): List[Datapoint] = {
    val builder = List.newBuilder[Datapoint]
    foreachItem(parser) {
      builder += decode(parser, null, intern)
    }
    builder.result
  }

  def decodeList(json: String): List[Datapoint] = {
    val parser = Json.newJsonParser(json)
    try decodeList(parser, false) finally parser.close()
  }

  private def encodeTags(gen: JsonGenerator, tags: Map[String, String]) {
    gen.writeObjectFieldStart("tags")
    tags match {
      case m: SmallHashMap[String, String] => m.foreachItem { (k, v) => gen.writeStringField(k, v) }
      case m: Map[String, String]          => m.foreach { t => gen.writeStringField(t._1, t._2) }
    }
    gen.writeEndObject()
  }

  def encodeDatapoint(gen: JsonGenerator, d: Datapoint) {
    gen.writeStartObject()
    encodeTags(gen, d.tags)
    gen.writeNumberField("timestamp", d.timestamp)
    gen.writeNumberField("value", d.value)
    gen.writeEndObject()
  }

  def encodeDatapoint(d: Datapoint): String = {
    Streams.string { w =>
      Streams.scope(Json.newJsonGenerator(w)) { gen => encodeDatapoint(gen, d) }
    }
  }

  def encodeBatch(gen: JsonGenerator, tags: Map[String, String], values: List[Datapoint]) {
    gen.writeStartObject()
    encodeTags(gen, tags)
    gen.writeArrayFieldStart("metrics")
    values.foreach(v => encodeDatapoint(gen, v))
    gen.writeEndArray()
    gen.writeEndObject()
  }

  def encodeBatch(tags: Map[String, String], values: List[Datapoint]): String = {
    Streams.string { w =>
      Streams.scope(Json.newJsonGenerator(w)) { gen => encodeBatch(gen, tags, values) }
    }
  }

  case class PublishRequest(values: List[Datapoint], failures: List[ValidationResult])

  case class FailureMessage(`type`: String, errorCount: Int, message: List[String]) extends JsonSupport {
    def typeName: String = `type`
  }

  object FailureMessage {
    def error(message: List[ValidationResult]): FailureMessage = {
      val failures = message.collect { case ValidationResult.Fail(_, reason) => reason }
      new FailureMessage(DiagnosticMessage.Error, failures.size, failures.take(5))
    }

    def partial(message: List[ValidationResult]): FailureMessage = {
      val failures = message.collect { case ValidationResult.Fail(_, reason) => reason }
      new FailureMessage("partial", failures.size, failures.take(5))
    }
  }
}
