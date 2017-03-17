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
import java.util.concurrent.atomic.AtomicLong

// for auto schedule of snapshots
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import java.math.BigInteger

// kafka
import akka.kafka.{ProducerMessage,ProducerSettings}
import akka.stream.ActorMaterializer
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

object ClusteredPublishActor{
  import com.netflix.atlas.webapi.PublishApi.PublishRequest
  import com.netflix.atlas.config.ConfigManager

  private val logger = LoggerFactory.getLogger(getClass)

  def shardName = "ClusteredPublishActor"
  private val config = ConfigManager.current.getConfig("atlas.akka.atlascluster")
  // Kafka Config
  private val kafkaConfig = ConfigManager.current.getConfig("akka.kafka.producer")
  private val kafkaEnabled = config.getBoolean("kafka-enabled")
  // typically atlas-metrics
  private val kafkaTopicName = config.getString("kafka-topic-name")
  private val KafkaMetadataMaxAgeMS = config.getInt("kafka-metadata-max-age-ms")
  // shard config
  private val numberOfShards = config.getInt("number-of-shards")
  // persistence
  private val retainMinutes = config.getInt("retain-minutes")

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
  def updatedRaw(rawEvents: List[String]): ClusterPublishState = copy (rawEvents ++ events)

  def size: Int = events.length
  override def toString: String = events.reverse.toString
}


class ClusteredPublishActor(registry: Registry, db: Database) extends PersistentActor with ActorLogging {
  import com.netflix.atlas.webapi.PublishApi.PublishRequest
  import com.netflix.atlas.webapi.PublishApi.FailureMessage

  import ClusteredPublishActor._

  import scala.concurrent.duration._

  private val memDb = db.asInstanceOf[MemoryDatabase]

  // Track the ages of data flowing into the system. Data is expected to arrive quickly and
  // should hit the backend within the step interval used.
  private val numReceived = {
    val f = BucketFunctions.age(DefaultSettings.stepSize, TimeUnit.MILLISECONDS)
    BucketCounter.get(registry, registry.createId("atlas.db.numMetricsReceived"), f)
  }

  // Number of invalid datapoints received
  private val numInvalid = registry.createId("atlas.db.numInvalid")

  /** Tracks how many invalid datapoints received */
  private val numDiscardedDatapoints = registry.counter("atlas.publish.numDiscardedDatapoints")
  /** Tracks how many metrics have been sent to kafka */
  private val numKafkaSendSuccess = registry.counter("atlas.publish.kafka.sendSuccess")
  private val numKafkaSendFailures = registry.counter("atlas.publish.kafka.sendFailures")
  private val kafkaProducerActive = registry.counter("atlas.publish.kafka.producer.active")
  

  private val cache = new NormalizationCache(DefaultSettings.stepSize, memDb.update)

  override def persistenceId = self.path.toStringWithoutAddress

  private val actorShardId = registry.createId("atlas.shard.id")

  // record our persistenceId
  var shardNumber = -1L
  try {
    // need to get the shard# from the persistenceId, then increase the counter to match
    // persistenceId is in the form: "/system/sharding/ClusteredPublishActor/11/11"
    shardNumber = persistenceId.split("/")(5).toLong
    registry.counter(actorShardId.withTag("persistenceId", persistenceId)).increment(shardNumber)
  }
  catch {
    case e: Exception =>
      log.error(s"Cannot determine shardid from persistenceId ${persistenceId}:" + e)
  }
  var state = ClusterPublishState()

  /*
   *  Journal Sequences are stored in a SortedMap
   */
  var journalHistory = scala.collection.SortedMap[Long,Long]()

  // retain-minutes is defined in the config file
  val retentionInterval = retainMinutes * 60 * 1000
  var recoveryCounter: Long = 0
  val snapshotDelay = 10.minutes
  val snapshotInterval = 5.minutes

  def updateState(event: ClusterPublishEvt): Long = {
    val purgeTimestamp = System.currentTimeMillis - retentionInterval
    val datapoints = Json.decode[List[Datapoint]](event.data)
    if (datapoints.head.timestamp >= purgeTimestamp) {
      updateCache(datapoints)
      1L
    } else {
      0L
    }
  }

  def numEvents =
    state.size

  // needed for kafka
  implicit val materializer = ActorMaterializer()
  // single producer
  var aProducer: KafkaProducer[Array[Byte], String] = null
  var producerSettings : ProducerSettings[Array[Byte], String] = null

  def createKafkaProducer() {
   log.info(s"### Creating KAFKA Producer using client.id: ${persistenceId}")
   producerSettings = ProducerSettings(kafkaConfig, new ByteArraySerializer, new StringSerializer)
      .withProperty("client.id", persistenceId)
      .withProperty("metadata.max.age.ms", KafkaMetadataMaxAgeMS.toString())

    aProducer = producerSettings.createKafkaProducer()
    kafkaProducerActive.increment()
  }

  def sendToKafka(evt: ClusterPublishEvt) = {
    if (aProducer == null) {
      createKafkaProducer()
    }
    log.debug("### Sending event to KAFKA")
    try {
      val ba = List(evt.data)
      val done = Source(ba)
        .map { n =>
          // can distribute this message across partitions
          // val partition = math.abs(n) % 2
          //val partition = 0
          //ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
          //  "atlas-metrics", partition, null, n.toString
          //), n)
          // See https://kafka.apache.org/0101/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
          // for constructor options, this one forces kafka to round robin (no partition or key is supplied)
          ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
            kafkaTopicName, n.toString
          ), n)
        }
        .via(Producer.flow(producerSettings,aProducer))
        .map { result =>
          val record = result.message.record
          log.debug(s"ClusteredPublishActor.sendToKafka: ${record.topic}/${record.partition} ${result.offset}: ${record.value}" +
          s"(${result.message.passThrough})")
          numKafkaSendSuccess.increment()
        }
        .runWith(Sink.ignore)
    }
    catch {
      case e: Exception =>
        numKafkaSendFailures.increment()
        log.warning("Failed to send message to Kafka: " + e.getMessage)
    }
  }

  override def receiveRecover: Receive = {
    case evt: ClusterPublishEvt =>
      recoveryCounter += updateState(evt)
    case SnapshotOffer(metadata, offeredSnapshot: ClusterPublishState) =>
      log.info("### Snapshot offered - ignoring and using journal")
      //log.info("### Snapshot offered - using it")
      //log.info("### Snapshot used - state size before: " + state.size)
      //state = offeredSnapshot
      //log.info("### Snapshot used - state size after: " + state.size)
    case RecoveryCompleted =>
      log.info(s"################# ClusteredPublishActor: recovery completed, replayed ${recoveryCounter} journal entries")
      recoveryCounter = 0L
  }

  override def receiveCommand: Receive = {
    case IngestTaggedItem(id,req) =>
      req match {
        case PublishRequest(Nil, Nil) =>
          log.warning("IngestTaggedItem:PublishRequest bad request - payload empty")
          DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty payload")
        case PublishRequest(Nil, failures) =>
          log.warning("IngestTaggedItem:PublishRequest onlyfailures")
          updateStats(failures)
          val msg = FailureMessage.error(failures)
          sendError(sender(), StatusCodes.BadRequest, msg)
        case PublishRequest(values, Nil) =>
          // respond first, then persist async
          sender() ! HttpResponse(StatusCodes.OK)
          persistAsync(ClusterPublishEvt(Json.encode(values))) {
            event =>
              updateState(event)
              context.system.eventStream.publish(event)
              if (kafkaEnabled) sendToKafka(event)
          }
        case PublishRequest(values, failures) =>
          // respond first, then persist async
          val msg = FailureMessage.partial(failures)
          sendError(sender(), StatusCodes.Accepted, msg)
          log.warning("IngestTaggedItem:PublishRequest partial failures")
          persistAsync(ClusterPublishEvt(Json.encode(values))) {
            event =>
              updateState(event)
              updateStats(failures)
              context.system.eventStream.publish(event)
              if (kafkaEnabled) sendToKafka(event)
          }
        case _ =>
          log.error("IngestTaggedItem: unknown request - " + req)
      }
    case "snap"  =>
      saveSnapshot(state)
    case SaveSnapshotSuccess(m) =>
      log.info(s"##### SaveSnapshotSuccess: seqNum:${m.sequenceNr}, timeStamp:${m.timestamp}")
      // store the timestamp and sequence in our history
      journalHistory += m.timestamp -> m.sequenceNr
      val timeNowMS: Long = System.currentTimeMillis
      val purgeTimestamp = timeNowMS - retentionInterval
      // The snapshot has everything up to m.SequenceNr, and time m.timestamp
      // purge all journal entries that are older than the snapshot
      //val purgeTimestamp = m.timestamp
      val journalHistorySizeBefore = journalHistory.size
      log.info(s"##### SaveSnapshotSuccess: journalHistory size before filter: ${journalHistorySizeBefore}")
      log.info(s"##### SaveSnapshotSuccess: journalHistory before filter: ${journalHistory}")
      journalHistory = journalHistory.filterKeys(_ >= purgeTimestamp)
      // if we did drop data from the journal, purge the messages
      if (journalHistory.size < journalHistorySizeBefore) {
        log.info(s"##### SaveSnapshotSuccess: journalHistory size after filter: ${journalHistory.size}")
        log.info(s"##### SaveSnapshotSuccess: journalHistory changed, is now: ${journalHistory}")
        // keep everything up to what is left, this is a sortedmap, the head will have the oldest timestamp
        val purgeSequence = journalHistory.head
        // we get back a (Long, Long), we want the sequence#
        log.info(s"###### SaveSnapshotSuccess: oldest journal entry seqNum:${purgeSequence._2}, timeStamp:${purgeSequence._1}")
        var purgeSequenceNumber = purgeSequence._2 - 1
        // make sure it is >= 0
        if (purgeSequenceNumber < 0) { purgeSequenceNumber = 0}
        // now purge the journal
        log.info(s"##### SaveSnapshotSuccess: Deleting journal entries up to sequence ${purgeSequenceNumber}")
        deleteMessages(purgeSequenceNumber)
      } else {
        log.info(s"##### SaveSnapshotSuccess: Retaining ALL journal entries")
        log.info(s"##### SaveSnapshotSuccess: journalHistory contains: ${journalHistory}")
        log.info(s"##### SaveSnapshotSuccess: journalHistory size: ${journalHistorySizeBefore}")
      }
      // now delete this snapshot
      log.info(s"##### Deleting old snapshots...")
      deleteSnapshots(new SnapshotSelectionCriteria(m.sequenceNr, m.timestamp))
      log.info(s"##### Deleting old snapshot completed")
    case ShutdownClusteredPublisher =>
      context.stop(self)
  }

  private def sendError(ref: ActorRef, status: StatusCode, msg: FailureMessage): Unit = {
    val entity = HttpEntity(MediaTypes.`application/json`, msg.toJson)
    ref ! HttpResponse(status = status, entity = entity)
  }

  private def updateCache(vs: List[Datapoint]): Unit = {
    val now = System.currentTimeMillis()
    vs.foreach { v =>
      numReceived.record(now - v.timestamp)
      try {
        v.tags.get(TagKey.dsType) match {
          case Some("counter") => cache.updateCounter(v)
          case Some("gauge")   => cache.updateGauge(v)
          case Some("rate")    => cache.updateRate(v)
          case _               => cache.updateRate(v)
        }
        //log.error(s"Ingestion OK, payload is: ${v}")
      }
      catch {
       case e: Exception =>
          log.debug("skipping ingestion, error is:", e)
          log.debug(s"skipping ingestion, vs payload is: ${vs}")
      }
    }
  }

  private def updateStats(failures: List[ValidationResult]): Unit = {
    failures.foreach {
      case ValidationResult.Pass           => // Ignored
      case ValidationResult.Fail(error, _) =>
        numDiscardedDatapoints.increment()
        registry.counter(numInvalid.withTag("error", error))
    }
  }

  // Take a snapshot every snapshotInterval minutes... after waiting snapshotDelay minutes after startup
  context.system.scheduler.schedule(snapshotDelay, snapshotInterval, self, "snap")(context.system.dispatcher, self)
}
