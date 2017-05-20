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

import java.awt.Color
import java.io.ByteArrayOutputStream
import java.time.Duration

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.ActorRef
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.chart._
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model._
import com.netflix.atlas.core.model._
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Strings
import com.netflix.spectator.api.Registry
import spray.can.Http
import spray.http.MediaTypes._
import spray.http._
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion._
import akka.cluster.sharding.{ClusterShardingSettings, ClusterSharding}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import java.math.BigInteger


class GraphRequestActor(registry: Registry, system: ActorSystem) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.GraphApi._
  import com.netflix.atlas.core.index.TagQuery
  import com.netflix.atlas.config.ConfigManager

  private val errorId = registry.createId("atlas.graph.errorImages")

  val dbRef = ClusterSharding(context.system).shardRegion(ClusteredDatabaseActor.shardName)

  private val config = ConfigManager.current.getConfig("atlas.akka.atlascluster")
  private val numberOfShards = config.getInt("number-of-shards")

  private var request: Request = _
  private var responseRef: ActorRef = _

  def receive = {
    case v => try innerReceive(v) catch {
      case t: Exception if request != null && request.shouldOutputImage =>
        // When viewing a page in a browser an error response is not rendered. To make it more
        // clear to the user we return a 200 with the error information encoded into an image.
        sendErrorImage(t, request.flags.width, request.flags.height)
      case t: Throwable =>
        DiagnosticMessage.handleException(responseRef)(t)
    }
  }

  def innerReceive: Receive = {
    case req: Request =>
      // important - this is referenced elsewhere and must be first
      responseRef = sender()
      //log.info("GraphRequestActor.req GetShardedData: START Processing Request: " + req.hashCode())
      log.info("GraphRequestActor.req GetShardedData: START Processing Request: {} sender is {}",req.hashCode(), responseRef.toString())
      val t0 = System.nanoTime()
      request = req
      /* the request can be inspected for the tags that will be queried, and only the shards that have those tags need be asked */
      //var shardsToAsk = getShardsForQuery(req)
      //log.info("GraphRequestActor.req: request is " + req)
      var results: List[DataResponse] = List()
      var dbRequest = req.toDbRequest

      // ask all shards
      val shardList = List.range(0,numberOfShards)
      val futureMap = shardList.map {
        shardId =>
          //log.debug("GraphRequestActor.req GetShardedData: asking shard: " + shardId)
          val aFuture = dbRef.ask(ClusteredDatabaseActor.GetShardedData(shardId, dbRequest))(20.seconds).mapTo[DataResponse]
          aFuture
      }
      
      // store all of the futures in a future sequence
      val futureSequence = Future.sequence(futureMap)
      
      // this is a new future that will pull together all of the shard requests
      // this is nonblocking since it is itself a future
      val master = futureSequence.map {
        responses =>
          responses.foreach { aResponse =>
            results = aResponse :: results
          }
      }
      
      // when the master has finished its mapping, we're done with shard requests
      // merge the results and send back the data
      master onComplete{
        case l => {
          //log.debug("GraphRequestActor.req: master onComplete entered")
          log.info("GraphRequestActor.req GetShardedData: ONCOMPLETE Entered Processing Request: " + req.hashCode())

          // we have all responses now
          //log.debug("GraphRequestActor.req: master: result as list is " + results)
          /*
           * The merge process requires matching data expressions (since there may be multiple)
           * and the merging of time series that have matching tags
           */
          //log.debug("GraphRequestActor.req: master: Merging...")
          var mergedData = scala.collection.mutable.Map[DataExpr, List[TimeSeries]]()
          var validData = scala.collection.mutable.Map[DataExpr, List[TimeSeries]]()
          // we may get back an expanded dataexpression, and will need to adjust the merged data as the key will be different
          // prepopulate the mergedData with all of the data expressions found in the results
          results.foreach {
            aDataResponse =>
              aDataResponse.ts.keys.foreach {
                key =>
                  if (!mergedData.contains(key)) {
                    // add the data expression into the mergedData map, with an empty time series list
                    //log.debug("GraphRequestActor.req: master: DATAEXPR FROM RESULT: " + key)
                    mergedData.put(key, List[TimeSeries]())
                    validData.put(key, List[TimeSeries]())
                  }
              }
          }
          // add the data expression into the mergedData map, with an empty time series list
          //mergedData.put(queryDataExpr, List[TimeSeries]())
          // 
          // DATA RESPONSE FILTERING
          //
          // get the list of timeseries that have data
          log.info("GraphRequestActor.req GetShardedData: FILTERING Processing Request: " + req.hashCode())

          results.foreach { aDataResponse =>
            aDataResponse.ts.foreach{ item =>
              // check the size of the List in the Map
              var k = item._1
              var v = item._2
              //log.debug("GraphRequestActor.req: master: DataResponse DataExpr is " + k)
              if (v.size > 0) {
                // Skip any result with with NO_DATA inside the label
                v.foreach { ts =>
                  if (!ts.label.equals("NO DATA")) {
                    //log.debug("GraphRequestActor.req: master: tags of timeseries: " + ts.tags.toString());
                    // append
                    //log.debug("GraphRequestActor.req: master: Appending ts to valid data, ts: " + ts)
                    //
                    // get the timeseries
                    var newData = List[TimeSeries]()
                    if (validData.contains(k)) {
                      // this dataexpr already exist, get the current timeseries list from validData
                      val currentSeries = validData.get(k)
                      // append to newData
                      newData = currentSeries.toList.flatten ++ newData
                    }
                    // append timeseries to newData (this will contain whatever it had before,
                    // plus data from this result
                    newData = newData ++ v
                    validData.put(k, newData)
                  }
                }
              }
            }
          }
          // Append/Merge as needed
          //log.debug("GraphRequestActor.req: master: ****** Merging shard response, mergedData is: " + mergedData)
          //log.debug("GraphRequestActor.req: master: ****** Merging shard response, validData is: " + validData)
          log.info("GraphRequestActor.req GetShardedData: MERGING Processing Request: " + req.hashCode())

          validData.foreach {
            validDataItem =>
              val validDataItemDataExpr = validDataItem._1
              //val validDataItemTSList = validDataItem._2
              validDataItem._2.foreach {
                aTimeSeries :TimeSeries =>
                  val validDataItemTSTags = aTimeSeries.tags
                  //log.debug("GraphRequestActor.req: master: ****** VALID_DATA_ITEM: Expr {} TAGS {}", validDataItemDataExpr, validDataItemTSTags)
                  // check if the tags for this dataItem already exist in the mergedData's list of timeseries
                  var storedTimeSeries = List[TimeSeries]()
                  if (mergedData.contains(validDataItemDataExpr)) {
                    storedTimeSeries = mergedData(validDataItemDataExpr)
                  }
                  // iterate over the stored time series
                  var foundMatch = false
                  storedTimeSeries.foreach {
                    aTS =>
                      //log.debug("GraphRequestActor.req: master: ****** STORED_TIME_SERIES TAGS {} vs VALID_DATA_ITEM TAGS {} ", aTS.tags, validDataItemTSTags)
                      if (aTS.tags == validDataItemTSTags) {
                        foundMatch = true
                        //log.debug("GraphRequestActor.req: master: ****** MATCHING TAGS FOUND, blending existing data: " + mergedData)
                        // tags matched, blend the storedData with the new data
                        var newStoredTimeSeries: List[TimeSeries] = List()
                        //var blendedSeries = List[TimeSeries](aTS.blend(validDataItemTSList))
                        //log.info("GraphRequestActor.req: master: ****** BLEND blended series list is " + blendedSeries)
                        var newBlendedSeries = aTS.blend(aTimeSeries)
                        //log.debug("GraphRequestActor.req: master: ****** BLEND newBendedSeries is " + newBlendedSeries)
                        // overwrite the old data
                        mergedData.put(validDataItemDataExpr,List(newBlendedSeries))
                      }
                  }
                  // this new data wasn't in the storedTimeSeries, so just append it
                  if (!foundMatch) {
                    //log.debug("GraphRequestActor.req: master: ****** NO MATCHING TAGS FOUND in storedTimeSeries, appending " + validDataItem)
                    //log.debug("GraphRequestActor.req: master: ****** before append, mergedData is: " + mergedData)
                    var newStoredTimeSeries: List[TimeSeries] = List()
                    newStoredTimeSeries = newStoredTimeSeries ++ storedTimeSeries
                    newStoredTimeSeries = aTimeSeries :: newStoredTimeSeries
                    mergedData.put(validDataItemDataExpr, newStoredTimeSeries)
                    //log.debug("GraphRequestActor.req: master: ****** NO MATCHING TAGS FOUND, after append, mergedData is: " + mergedData)
                  }              

              }
          }
          //log.debug("GraphRequestActor.req: master: FINAL mergedData is " + mergedData)
          val t1 = System.nanoTime()
          log.info("GraphRequestActor.req GetShardedData: FINISHED Processing Request: {} elapsed NS {} ", req.hashCode(), (t1-t0))
          sendImage(mergedData.toMap)
        }
      }
      // best effort - if there's a failure on an ask, discard it
      master onFailure {
        case l => {
          log.warning("GraphRequestActor.req: master onFailure: " + l)
        }
      }
    case DataResponse(data) =>
      //log.debug("Data is " + data)
      sendImage(data)
    case ev: Http.ConnectionClosed =>
      log.debug("connection closed")
      context.stop(self)
  }

  /*
   * This returns a List[Integer]() of all shards corresponding to the tags of the query
   */
  private def getShardsForQuery(req: Request) : List[Integer] = {
    var allShards = List[Integer]()
    req.exprs.foreach {
      aStyleExpr =>
        //log.info("****aStyleExpr = " + aStyleExpr)
        aStyleExpr.expr.dataExprs.foreach {
          aDataExpr =>
            var aQueryList = Query.cnfList(aDataExpr.query)
            aQueryList.foreach {
              aQuery =>
                //log.info("aQueryList pair = " + aQuery)
                var tagsMap = Query.tags(aQuery)
                //log.info("** aQueryList tags = " + tagsMap)
                //tagsMap.values.foreach {
                //  aValue =>
                //    log.info("TAG IS " + aValue)
                //}
                var ti = TaggedItem.computeId(tagsMap)
                //log.info("TaggedItem computed ID is " + ti)
                var shardId: BigInteger = BigInteger.ZERO
                // old way using all tags
                //var shardId = ti.abs().mod(BigInteger.valueOf(numberOfShards))
                tagsMap.foreach {
                  aTag =>
                    if (aTag._1 == "name") {
                      // found name, get its value
                      var aTagName = aTag._2
                      // create a new Map
                      var justName = Map[String,String](aTag._1  -> aTag._2 )
                      // now compute the shard id
                      log.info("GRA: Computing shardId for name: " + justName)
                      ti = TaggedItem.computeId(justName)
                      shardId = ti.abs().mod(BigInteger.valueOf(numberOfShards))
                      log.info("GRA: Compute shardId is: " + shardId)

                      // break - TODO
                    }
                }
                log.info("GRA: Appending shardId: " + shardId)
                allShards = shardId.intValue() :: allShards
            }
        }
    }
    // reduce to distinct shards
    allShards = allShards.distinct
    return allShards
  }

  private def sendErrorImage(t: Throwable, w: Int, h: Int): Unit = {
    val simpleName = t.getClass.getSimpleName
    registry.counter(errorId.withTag("error", simpleName)).increment()
    val msg = s"$simpleName: ${t.getMessage}"
    val image = HttpEntity(`image/png`, PngImage.error(msg, w, h).toByteArray)
    responseRef ! HttpResponse(status = StatusCodes.OK, entity = image)
  }

  private def sendImage(data: Map[DataExpr, List[TimeSeries]]): Unit = {
    val warnings = List.newBuilder[String]

    val plotExprs = request.exprs.groupBy(_.axis.getOrElse(0))
    val multiY = plotExprs.size > 1

    val palette = newPalette(request.flags.palette)
    val shiftPalette = newPalette("bw")

    val start = request.startMillis
    val end = request.endMillis

    val plots = plotExprs.toList.sortWith(_._1 < _._1).map { case (yaxis, exprs) =>
      val axisCfg = request.flags.axes(yaxis)
      val dfltStyle = if (axisCfg.stack) LineStyle.STACK else LineStyle.LINE

      val axisPalette = axisCfg.palette.fold(palette) { v => newPalette(v) }

      var messages = List.empty[String]
      val lines = exprs.flatMap { s =>
        val result = s.expr.eval(request.evalContext, data)

        // Pick the last non empty message to appear. Right now they are only used
        // as a test for providing more information about the state of filtering. These
        // can quickly get complicated when used with other features. For example,
        // sorting can mix and match lines across multiple expressions. Also binary
        // math operations that combine the results of multiple filter expressions or
        // multi-level group by with filtered input. For now this is just an
        // experiment for the common simple case to see how it impacts usability
        // when dealing with filter expressions that remove some of the lines.
        if (result.messages.nonEmpty) messages = result.messages.take(1)

        val ts = result.data
        val labelledTS = ts.map { t =>
          val offset = Strings.toString(Duration.ofMillis(s.offset))
          val newT = t.withTags(t.tags + (TagKey.offset -> offset))
          newT.withLabel(s.legend(newT))
        }

        val lineDefs = labelledTS.sortWith(_.label < _.label).map { t =>
          val color = s.color.getOrElse {
            val c = if (s.offset > 0L) shiftPalette(t.label) else axisPalette(t.label)
            // Alpha setting if present will set the alpha value for the color automatically
            // assigned by the palette. If using an explicit color it will have no effect as the
            // alpha can be set directly using an ARGB hex format for the color.
            s.alpha.fold(c)(a => Colors.withAlpha(c, a))
          }

          LineDef(
            data = t,
            color = color,
            lineStyle = s.lineStyle.fold(dfltStyle)(s => LineStyle.valueOf(s.toUpperCase)),
            lineWidth = s.lineWidth,
            legendStats = SummaryStats(t.data, start, end))
        }

        // Lines must be sorted for presentation after the colors have been applied
        // using the palette. The colors selected should be stable regardless of the
        // sort order that is applied. Otherwise colors would change each time a user
        // changed the sort.
        sort(warnings, s.sortBy, s.useDescending, lineDefs)
      }

      // Apply sort based on URL parameters. This will take precedence over
      // local sort on an expression.
      val sortedLines = sort(warnings, axisCfg.sort, axisCfg.order.contains("desc"), lines)

      PlotDef(
        data = sortedLines ::: messages.map(s => MessageDef(s"... $s ...")),
        lower = axisCfg.lower.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
        upper = axisCfg.upper.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
        ylabel = axisCfg.ylabel,
        scale = if (axisCfg.logarithmic) Scale.LOGARITHMIC else Scale.LINEAR,
        axisColor = if (multiY) None else Some(Color.BLACK),
        tickLabelMode = axisCfg.tickLabels.fold(TickLabelMode.DECIMAL)(TickLabelMode.apply))
    }

    val graphDef = request.newGraphDef(plots, warnings.result())

    val baos = new ByteArrayOutputStream
    request.engine.write(graphDef, baos)

    val entity = HttpEntity(request.contentType, baos.toByteArray)
    responseRef ! HttpResponse(StatusCodes.OK, entity)
    context.stop(self)
  }

  /**
    * Creates a new palette and optionally changes it to use the label hash for
    * selecting the color rather than choosing the next available color in the
    * palette. Hash selection is useful to ensure that the same color is always
    * used for a given label even on separate graphs. However, it also means
    * that collisions are more likely and that the same color may be used for
    * different labels even with a small number of lines.
    *
    * Hash mode will be used if the palette name is prefixed with "hash:".
    */
  private def newPalette(mode: String): String => Color = {
    val prefix = "hash:"
    if (mode.startsWith(prefix)) {
      val pname = mode.substring(prefix.length)
      val p = Palette.create(pname)
      v => p.colors(v.hashCode)
    } else {
      val p = Palette.create(mode).iterator
      _ => p.next()
    }
  }

  private def sort(
      warnings: scala.collection.mutable.Builder[String, List[String]],
      sortBy: Option[String],
      useDescending: Boolean,
      lines: List[LineDef]): List[LineDef] = {
    sortBy.fold(lines) { mode =>
      val cmp: Function2[LineDef, LineDef, Boolean] = mode match {
        case "legend" => (a, b) => a.data.label < b.data.label
        case "min"    => (a, b) => a.legendStats.min < b.legendStats.min
        case "max"    => (a, b) => a.legendStats.max < b.legendStats.max
        case "avg"    => (a, b) => a.legendStats.avg < b.legendStats.avg
        case "count"  => (a, b) => a.legendStats.count < b.legendStats.count
        case "total"  => (a, b) => a.legendStats.total < b.legendStats.total
        case "last"   => (a, b) => a.legendStats.last < b.legendStats.last
        case order    =>
          warnings += s"Invalid sort mode '$order'. Using default of 'legend'."
          (a, b) => a.data.label < b.data.label
      }
      val sorted = lines.sortWith(cmp)
      if (useDescending) sorted.reverse else sorted
    }
  }
}

