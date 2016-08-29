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

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.persistence._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.db.MemoryDatabase
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.norm.NormalizationCache
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.BucketCounter
import com.netflix.spectator.api.histogram.BucketFunctions
import spray.http.HttpEntity
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.http.StatusCode
import spray.http.StatusCodes
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import com.netflix.atlas.json.Json

// for auto schedule of snapshots
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import java.math.BigInteger

object ClusteredPublishActor{
  import com.netflix.atlas.webapi.PublishApi._
  import com.netflix.atlas.config.ConfigManager
  def shardName = "ClusteredPublishActor"
  private val config = ConfigManager.current.getConfig("atlas.akka.atlascluster")
  private val numberOfShards = config.getInt("number-of-shards")
  private val bignumberOfShards: BigInteger = BigInteger.valueOf(numberOfShards)

  case class IngestTaggedItem(taggedItemId: BigInteger, req: PublishRequest)

  val extractShardId: ShardRegion.ExtractShardId = msg => msg match {
    case IngestTaggedItem(taggedItemId: BigInteger, req: PublishRequest) =>
      println("************** IngestTaggedItem IngestTaggedItem extract shardid *********")
      var shardId = taggedItemId.abs().mod(bignumberOfShards)
      println("************** IngestTaggedItem IngestTaggedItem extract shardid = " + shardId)
      shardId.toString
  }
  
  val extractEntityId: ExtractEntityId = {
    case d: IngestTaggedItem =>  (d.taggedItemId.toString(), d)
  }
}

case object ShutdownClusteredPublisher
case class ClusterPublishCmd(data: String)
case class ClusterPublishEvt(data: String)

case class ClusterPublishState(events: List[String] = Nil) {
  def updated(evt: ClusterPublishEvt): ClusterPublishState = copy(evt.data :: events)
  def size: Int = events.length
  override def toString: String = events.reverse.toString
}


class ClusteredPublishActor(registry: Registry, db: Database) extends PersistentActor with ActorLogging {
  import ShardRegion.Passivate
  import com.netflix.atlas.webapi.PublishApi._
  import ClusteredPublishActor._
 
  import scala.concurrent.duration._
  // TODO: This actor is only intended to work with MemoryDatabase, but the binding is
  // setup for the Database interface.
  private val memDb = db.asInstanceOf[MemoryDatabase]

  // passivate the entity when no activity
  //context.setReceiveTimeout(2.minutes)
  
  // Track the ages of data flowing into the system. Data is expected to arrive quickly and
  // should hit the backend within the step interval used.
  private val numReceived = {
    val f = BucketFunctions.age(DefaultSettings.stepSize, TimeUnit.MILLISECONDS)
    BucketCounter.get(registry, registry.createId("atlas.db.numMetricsReceived"), f)
  }

  // Number of invalid datapoints received
  private val numInvalid = registry.createId("atlas.db.numInvalid")

  private val cache = new NormalizationCache(DefaultSettings.stepSize, memDb.update)

  override def persistenceId = self.path.parent.name + "-" + self.path.name

 
  var state = ClusterPublishState()
 
  var lastSnapshot: SnapshotMetadata = null
  var lastSnapshotSequenceNum: Long = 0
  var lastSnapshotTimestamp: Long = 0
  var lastSnapshotSize: Int = 0
  var futureSnapshotSize: Int = 0

  def updateState(event: ClusterPublishEvt): Unit =
    state = state.updated(event)
 
  def numEvents =
    state.size
 
  val receiveRecover: Receive = {
    case evt: ClusterPublishEvt =>
      updateState(evt)
      println("I should be putting stuff back into the memory database...")
      println("evt data is " + evt.data)
      var myDatapoints: List[Datapoint] = Json.decode[List[Datapoint]](evt.data)
      println("Mydatapoints is " + myDatapoints.toString())
      update(myDatapoints)
    case SnapshotOffer(metadata, snapshot: ClusterPublishState) =>
      println("Lets try to use a snapshot... with size " + snapshot.size)
      state = snapshot
      // on initial startup the sequence is unknown
      if (lastSnapshotSequenceNum == 0) {
        lastSnapshotSequenceNum = metadata.sequenceNr
        lastSnapshotTimestamp = metadata.timestamp
      }
      println("ok, we used a snapshot... when recovery is completed, make sure to populate memorydb!")
    case RecoveryCompleted =>
      println("recovery completed... now populate the memorydb")
      // we also need to load up our memory database with the snapshot data
      state.events.foreach {
        datapoint =>
          var recoverDatapoints: List[Datapoint] = Json.decode[List[Datapoint]](datapoint)
          // add them to memory db
          if (recoverDatapoints.size > 0) {
            update(recoverDatapoints)
          }
      }
      // we used a snapshot, get its size
      lastSnapshotSize = state.size

  }
  

 
  val receiveCommand: Receive = {
    case IngestTaggedItem(id,req) =>
      req match {
        case PublishRequest(Nil, Nil) =>
          println("IngestTaggedItem:PublishRequest badrequest")
          DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty payload")
        case PublishRequest(Nil, failures) =>
          println("IngestTaggedItem:PublishRequest onlyfailures")
          updateStats(failures)
          val msg = FailureMessage.error(failures)
          sendError(sender(), StatusCodes.BadRequest, msg)
        case PublishRequest(values, Nil) =>
          println("IngestTaggedItem:PublishRequest all good")
          update(values)
          sender() ! HttpResponse(StatusCodes.OK)
        case PublishRequest(values, failures) =>
          println("IngestTaggedItem:PublishRequest partial failures")
          update(values)
          updateStats(failures)
          val msg = FailureMessage.partial(failures)
          sendError(sender(), StatusCodes.Accepted, msg)
      }
      /*
      println("IngestTaggedItem receiveCommand doing update")
      println("Sender is " + sender.toString())
      update(req.values)
      //persist(ClusterPublishEvt(Json.encode(req.values))) { event =>
      //  updateState(event)
      //  context.system.eventStream.publish(event)
      //}
      println("IngestMe sending back OK")
      sender() ! "OK"
      //StatusCodes.OK
      //actorRef ! HttpResponse(StatusCodes.OK)
       
       */
    case "snap"  =>
      println("Command is to take a snapshot...")
      // check if there are more values than we had before
      if (state.size > lastSnapshotSize) {
        println(s"Taking a snapshot, old size was " + lastSnapshotSize + " new size is " +  state.size)
        saveSnapshot(state)
        // save the size, if the snapshot succeeds this will become our
        // new last snapshot size
        futureSnapshotSize = state.size
      }
      else {
        println("No changes have been made, skipping snapshot")
      }
    case SaveSnapshotSuccess(m) =>
      println(s"snapshot saved. seqNum:${m.sequenceNr}, timeStamp:${m.timestamp}")
      // check if the sequence number has changed, if it has, then we want to purge all of the old snapshots
      if (m.sequenceNr > lastSnapshotSequenceNum) {
        println(s"Sequence has increased, will purge old snapshots, old seq was " + lastSnapshotSequenceNum + " new seq is " +  m.sequenceNr)
        // delete our old snapshots
        deleteSnapshots(new SnapshotSelectionCriteria(lastSnapshotSequenceNum, lastSnapshotTimestamp))
        //deleteSnapshot(lastSnapshotSequenceNum)
        // track this snapshot
        lastSnapshot = m
        lastSnapshotSequenceNum = m.sequenceNr
        lastSnapshotTimestamp = m.timestamp
        // use the size we stored when taking the snapshot
        lastSnapshotSize = futureSnapshotSize
      }
    case ShutdownClusteredPublisher =>
      context.stop(self)
    case PublishRequest(Nil, Nil) =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty payload")
    case PublishRequest(Nil, failures) =>
      updateStats(failures)
      val msg = FailureMessage.error(failures)
      sendError(sender(), StatusCodes.BadRequest, msg)
    case PublishRequest(values, Nil) =>
      update(values)
      sender() ! HttpResponse(StatusCodes.OK)
    case PublishRequest(values, failures) =>
      update(values)
      updateStats(failures)
      val msg = FailureMessage.partial(failures)
      sendError(sender(), StatusCodes.Accepted, msg)
  }
  
  private def sendError(ref: ActorRef, status: StatusCode, msg: FailureMessage): Unit = {
    val entity = HttpEntity(MediaTypes.`application/json`, msg.toJson)
    ref ! HttpResponse(status = status, entity = entity)
  }
  
  private def update(vs: List[Datapoint]): Unit = {
    val now = System.currentTimeMillis()
    vs.foreach { v =>
      numReceived.record(now - v.timestamp)
      v.tags.get(TagKey.dsType) match {
        case Some("counter") => cache.updateCounter(v)
        case Some("gauge")   => cache.updateGauge(v)
        case Some("rate")    => cache.updateRate(v)
        case _               => cache.updateRate(v)
      }
    }
  }

  private def updateStats(failures: List[ValidationResult]): Unit = {
    failures.foreach {
      case ValidationResult.Pass           => // Ignored
      case ValidationResult.Fail(error, _) =>
        registry.counter(numInvalid.withTag("error", error))
    }
  }
  
  // we are going to snapshot every 30 seconds...
  //context.system.scheduler.schedule(60.seconds, 30.seconds, self, "snap")(context.system.dispatcher, self)
}

