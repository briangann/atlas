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
import akka.cluster.sharding.{ClusterShardingSettings, ClusterSharding}
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

class GraphRequestActor(registry: Registry, system: ActorSystem) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.GraphApi._
import com.netflix.atlas.core.index.TagQuery

  private val errorId = registry.createId("atlas.graph.errorImages")

  val dbRef = ClusterSharding(context.system).shardRegion(ClusteredDatabaseActor.shardName)

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

  /*
   * old crap
    case req: Request =>
      request = req
      responseRef = sender()
      // Need to pull out all of the tags that the query will use
      val dataExprs = req.exprs.flatMap(_.expr.dataExprs)
      val deduped = dataExprs.toSet.toList
      // println("deduped is " + deduped)
      var result: List[DataResponse] = List()
      println("There are " + deduped.size + " data expressions to process")
      deduped.foreach { x =>
        val pairs = Query.tags(x.query)
        // println("pairs is " + pairs)
        var ti = TaggedItem.computeId(pairs)
        // TODO: make this a comprehension-style future
        // this is slow, need to do these in parallel, plus handle exceptions
        // See http://doc.akka.io/docs/akka/snapshot/scala/futures.html#futures-scala
        println("Sending db req " + req.toDbRequest.toString())
        // TODO: GetShardData may not require the "self" parameter since it uses sender()
        val future = dbRef.ask(ClusteredDatabaseActor.GetShardData(ti, req.toDbRequest, self))(5.seconds)
        println("waiting....")
        // TODO: Handle exceptions
        var aresult: DataResponse = Await.result(future, 5.seconds).asInstanceOf[DataResponse]
        println("got something...")
        println("handleReq future response is " + aresult)
        // push into our list
        result = aresult :: result
      }
      //println("result as list is " + result)
      // now merge the results
      //println("All done, merging results")
      //println("Merging...")
      var mergedData = scala.collection.mutable.Map[DataExpr, List[TimeSeries]]()
      result.foreach { aDataResponse =>
        aDataResponse.ts.foreach{ item =>
          // check the size of the List in the Map
          var v = item._2
          // println("v is " + v)
          if (v.size > 0) {
            // println("this key/value was NOT empty, adding it")
            mergedData = mergedData ++ aDataResponse.ts
          }
        }
      }
      // println("mergedData is " + mergedData)
      //responseRef = sender()
      sendImage(mergedData.toMap)
  
  
   */
  def innerReceive: Receive = {
    case req: Request =>
      request = req
      responseRef = sender()
      println("GetShardedData: request is " + req)
      // ask all shards
      var results: List[DataResponse] = List()
      var dbRequest = req.toDbRequest
      println("GetShardedData: dbrequest is " + dbRequest)
      var shardId: Int = 0
      for (shardId <- 0 to ClusteredDatabaseActor.numberOfShards -1) {
        println("Sharded GetShardedData Request: Asking shard# " + shardId)
        val future = dbRef.ask(ClusteredDatabaseActor.GetShardedData(shardId, dbRequest))(5.seconds)
        println("Sharded GetShardedData Request: waiting....")
        // TODO: Handle exceptions
        var aresult: DataResponse = Await.result(future, 5.seconds).asInstanceOf[DataResponse]
        println("Sharded GetShardedData Request: future response is " + aresult)
        results = aresult :: results
      }
      println("result as list is " + results)
      // now merge the results
      println("All done, merging results")
      println("Merging...")
      var mergedData = scala.collection.mutable.Map[DataExpr, List[TimeSeries]]()
      results.foreach { aDataResponse =>
        aDataResponse.ts.foreach{ item =>
          // check the size of the List in the Map
          var k = item._1
          var v = item._2
          println("v is " + v)
          
          if (v.size > 0) {
            println("this key/value was NOT empty")
            // need to purge anything with NO_DATA inside
            var validData = List[TimeSeries]()
            v.foreach { ts =>
              if (ts.label.equals("NO DATA")) {
                // no data detected, not valid
                println("NO_DATA detected, skipping this result")
              }
              else {
                // append
                println("Appending valid data")
                validData = ts:: validData
              }
            }
            if (validData.size > 0) {
              // reduce to distinct values
              validData = validData.distinct
              println("Valid Data is " + validData)
              var newMap = Map[DataExpr,List[TimeSeries]](k ->validData)
              println("*****newMap is " + newMap)
              println("******mergedData was " + mergedData)
              mergedData = mergedData ++ newMap
              println("******mergedData is now " + mergedData)
            }
          }
        }
      }
      println("mergedData is " + mergedData)

      sendImage(mergedData.toMap)
    case DataResponse(data) =>
      println("Got DataResponse...")
      println("Data is " + data)
      sendImage(data)
    case ev: Http.ConnectionClosed =>
      log.info("connection closed")
      context.stop(self)
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

    val palette = Palette.create(request.flags.palette).iterator
    val shiftPalette = Palette.create("bw").iterator

    val start = request.startMillis
    val end = request.endMillis

    val plots = plotExprs.toList.sortWith(_._1 < _._1).map { case (yaxis, exprs) =>
      val axisCfg = request.flags.axes(yaxis)
      val dfltStyle = if (axisCfg.stack) LineStyle.STACK else LineStyle.LINE

      val axisPalette = axisCfg.palette.fold(palette) { v => Palette.create(v).iterator }

      val lines = exprs.flatMap { s =>
        val ts = s.expr.eval(request.evalContext, data).data
        val labelledTS = ts.map { t =>
          val offset = Strings.toString(Duration.ofMillis(s.offset))
          val newT = t.withTags(t.tags + (TagKey.offset -> offset))
          newT.withLabel(s.legend(newT))
        }

        val lineDefs = labelledTS.sortWith(_.label < _.label).map { t =>
          val color = s.color.getOrElse {
            val c = if (s.offset > 0L) shiftPalette.next() else axisPalette.next()
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
        data = sortedLines,
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

