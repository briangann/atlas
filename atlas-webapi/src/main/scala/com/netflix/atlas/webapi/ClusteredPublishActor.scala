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
/* kafka additions */
import akka.persistence.kafka._
import akka.persistence.kafka.journal.KafkaJournalConfig
import akka.serialization.SerializationExtension
/* end of kafka additions */
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
      //log.info("************** IngestTaggedItem IngestTaggedItem num shards = " + bignumberOfShards)
      //logger.info("************** IngestTaggedItem IngestTaggedItem extract shardid *********")
      var shardId = taggedItemId.abs().mod(bignumberOfShards)
      //logger.info("************** IngestTaggedItem IngestTaggedItem extract shardid = " + shardId)
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
  import com.netflix.atlas.webapi.PublishApi._
  import ClusteredPublishActor._
 
  import scala.concurrent.duration._

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

  override def persistenceId = self.path.parent.name + "_" + self.path.name

 
  var state = ClusterPublishState()
 
  var lastSnapshot: SnapshotMetadata = null
  var lastSnapshotSequenceNum: Long = 0
  var lastSnapshotTimestamp: Long = 0
  var lastSnapshotSize: Int = 0
  var futureSnapshotSize: Int = 0

  var recoveryCounter: Int = 0
  
  def updateState(event: ClusterPublishEvt): Unit =
    state = state.updated(event)
 
  def numEvents =
    state.size
 
  override def receiveRecover: Receive = {
    case _ =>
      log.info("receiveRecover: Receive: ########### GOT HERE! #######")
  }
  def NOTreceiveRecover: Receive = {
    case evt: ClusterPublishEvt =>
      //log.info("receiveRecover: Receive: case evt: ClusterPublishEvt: entered")
      updateState(evt)
      //log.info("ClusterPublishActor: adding evt to updateState")
      //log.info("I should be putting stuff back into the memory database...")
      //log.info("evt data is " + evt.data)
      var myDatapoints: List[Datapoint] = Json.decode[List[Datapoint]](evt.data)
      log.info("ClusteredPublishActor: Replay datapoint " + myDatapoints.toString())
      //log.info("receiveRecover: Receive: case evt: ClusterPublishEvt: calling update with old datapoints")
      update(myDatapoints)
      recoveryCounter += 1
      log.info("ClusteredPublishActor: recoveryCounter = " + recoveryCounter)
   case SnapshotOffer(metadata, snapshot: ClusterPublishState) =>
      log.info("Lets try to use a snapshot... with size " + snapshot.size)
      state = snapshot
      // on initial startup the sequence is unknown
      if (lastSnapshotSequenceNum == 0) {
        lastSnapshotSequenceNum = metadata.sequenceNr
        lastSnapshotTimestamp = metadata.timestamp
      }
      log.info("ok, we used a snapshot... when recovery is completed, make sure to populate memorydb!")
    case RecoveryCompleted =>
      log.info("################# ClusteredPublishActor: recoveryCounter = " + recoveryCounter)
      log.info("################# ClusteredPublishActor: recovery completed...")
      recoveryCounter = 0
      // TODO: if a snapshot was used to recover, we need to replay the state into the memory db
      // we also need to load up our memory database with the snapshot data
/*
      state.events.foreach {
        datapoint =>
          var recoverDatapoints: List[Datapoint] = Json.decode[List[Datapoint]](datapoint)
          // now add them to memory db
          if (recoverDatapoints.size > 0) {
            log.info("Recovery Completed - now pushing events into database " + recoverDatapoints.toString())
            update(recoverDatapoints)
          }
      }
      */
      
      // we used a snapshot, get its size
      lastSnapshotSize = state.size

  }
  

 override def receiveCommand: Receive = {
    case _ =>
      log.info("receiveCommand: Receive: ########### GOT HERE! #######")
  }
 def NOTreceiveCommand: Receive = {
    case IngestTaggedItem(id,req) =>
      req match {
        case PublishRequest(Nil, Nil) =>
          log.warning("IngestTaggedItem:PublishRequest badrequest")
          DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty payload")
        case PublishRequest(Nil, failures) =>
          log.warning("IngestTaggedItem:PublishRequest onlyfailures")
          updateStats(failures)
          val msg = FailureMessage.error(failures)
          sendError(sender(), StatusCodes.BadRequest, msg)
        case PublishRequest(values, Nil) =>
          //log.debug("IngestTaggedItem:PublishRequest all good")
          update(values)
          //persist(ClusterPublishEvt(Json.encode(values))) {
          //  event =>
          //    update(values)
          //    updateState(event)
          //    context.system.eventStream.publish(event)
          //}
          sender() ! HttpResponse(StatusCodes.OK)
        case PublishRequest(values, failures) =>
          log.warning("IngestTaggedItem:PublishRequest partial failures")
          update(values)
          updateStats(failures)
          //persist(ClusterPublishEvt(Json.encode(values))) {
          //  event =>
          //    updateState(event)
          //    update(values)
          //    updateStats(failures)
          //    context.system.eventStream.publish(event)
          //}
          val msg = FailureMessage.partial(failures)
          sendError(sender(), StatusCodes.Accepted, msg)
        case _ =>
          log.warning("IngestTaggedItem: unknown request - " + req)
      }
    case ClusterPublishCmd(data) =>
      log.info("ClusteredPublishActor: ClusterPublishCmd persist")
      //persist(ClusterPublishEvt(s"${data}-${numEvents}"))(updateState)
      //persist(ClusterPublishEvt(s"${data}-${numEvents + 1}")) { event =>
      //  updateState(event)
      //  context.system.eventStream.publish(event)
      //}
    case "snap"  =>
      log.info("Command is to take a snapshot...")
      // check if there are more values than we had before
      if (state.size > lastSnapshotSize) {
        log.info(s"Taking a snapshot, old size was " + lastSnapshotSize + " new size is " +  state.size)
        saveSnapshot(state)
        // save the size, if the snapshot succeeds this will become our
        // new last snapshot size
        futureSnapshotSize = state.size
      }
      else {
        log.info("No changes have been made, skipping snapshot")
      }
    case SaveSnapshotSuccess(m) =>
      log.info(s"snapshot saved. seqNum:${m.sequenceNr}, timeStamp:${m.timestamp}")
      // check if the sequence number has changed, if it has, then we want to purge all of the old snapshots
      if (m.sequenceNr > lastSnapshotSequenceNum) {
        log.info(s"Sequence has increased, will purge old snapshots, old seq was " + lastSnapshotSequenceNum + " new seq is " +  m.sequenceNr)
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
      //persist(ClusterPublishEvt(Json.encode(values))) { event =>
      //  updateState(event)
      //  context.system.eventStream.publish(event)
      //}
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
      try {
        log.info("ClusteredPublishActor: update storing " + v.toString())

        v.tags.get(TagKey.dsType) match {
          case Some("counter") => 
              cache.updateCounter(v)
          case Some("gauge")   => cache.updateGauge(v)
          case Some("rate")    => cache.updateRate(v)
          case _               => cache.updateRate(v)
        }
      }
      catch {
        case e: Exception =>
          log.error("skipping ingestion, error is:", e)
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

