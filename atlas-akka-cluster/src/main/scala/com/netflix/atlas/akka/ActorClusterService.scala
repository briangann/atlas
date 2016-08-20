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
package com.netflix.atlas.akkacluster

import javax.inject.Inject
import javax.inject.Singleton

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.actor.Props
import akka.routing.FromConfig
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.ClassFactory
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.netflix.atlas.webapi
/**
  * Exposes actor system as service for healthcheck and proper shutdown. Additional
  * actors to start up can be specified using the `atlas.akka.actors` property.
  */
@Singleton
class ActorClusterService @Inject() (system: ActorSystem, config: Config, classFactory: ClassFactory)
  extends AbstractService with StrictLogging {

  override def startImpl(): Unit = {
    import scala.collection.JavaConverters._
    
    config.getConfigList("atlas.akka.atlascluster").asScala.foreach { cfg =>
      val name = cfg.getString("name")
      val cls = Class.forName(cfg.getString("class"))
      // val myActor = newActor(name, cls)
      // use the name of the cluster 
      //def extractShardId: ExtractShardId = {case _ => name}      
      
      //def extractEntityId: ExtractEntityId = {case msg => (name, msg)}
      //val extractEntityId: ExtractEntityId = s"ENTITY_${name}_${cls.getName}"
      
      //val idExtractor: ExtractEntityId = {
      //  case cmd: Command => (cmd.postId, cmd)
     // }
      // publish and database are explicitly sharded as they appear in the config file
      // shard/cluster these two actors:
      // LocalDatabaseActor
      // LocalPublishActor
      
      //val extractEntityId: ExtractEntityId
      //val extractShardId: ExtractShardId
      //var clusterExtractShardId: akka.cluster.sharding.ShardRegion.ExtractShardId = null
      var clusterExtractEntityId: akka.cluster.sharding.ShardRegion.ExtractEntityId = null
      
      val numberOfShards = 2
      var shardid = (math.abs(name.hashCode) % numberOfShards).toString
      var clusterExtractShardId: akka.cluster.sharding.ShardRegion.ExtractShardId = {
        _ => (math.abs(name.hashCode) % numberOfShards).toString
      }
      
      logger.info(s"\n\n*******************shardid is ${shardid} for Actor ${cls.getName} *******************")       
      
      var knownActor: Boolean = true
      cls.getName match {
        case "com.netflix.atlas.webapi.LocalPublishActor" => 
          //clusterExtractShardId = com.netflix.atlas.webapi.LocalPublishActor.extractShardId
          clusterExtractEntityId = com.netflix.atlas.webapi.LocalPublishActor.extractEntityId
        case "com.netflix.atlas.webapi.LocalDatabaseActor" => 
          //clusterExtractShardId = com.netflix.atlas.webapi.LocalDatabaseActor.extractShardId
          clusterExtractEntityId = com.netflix.atlas.webapi.LocalDatabaseActor.extractEntityId
        case _ => knownActor = false
      }
      if (knownActor) { 
        ClusterSharding(system).start(
          typeName = name,
          entityProps = Props(classFactory.newInstance[Actor](cls)),
          settings = ClusterShardingSettings(system),
          extractShardId = clusterExtractShardId,
          extractEntityId = clusterExtractEntityId
        )

        val decider = ClusterSharding(system).shardRegion(name)      
        logger.info(s"\n\n*******************Created Actor in the new Cluster Actor*******************")       
        val ref = system.actorOf(newActor(name, cls), name)
        logger.info(cls.toString())
        logger.info(s"created actor '${ref.path}' using class '${cls.getName}'\n\n")
      }
      else {
        logger.info(s"\n\n*******************SKIPPED Unknown Actor ${cls.getName} in the new Cluster Actor*******************")      
      }
      
    }
  }

  private def newActor(name: String, cls: Class[_]): Props = {
    val props = Props(classFactory.newInstance[Actor](cls))
    val routerCfgPath = s"akka.actor.deployment./$name.router"
    if (config.hasPath(routerCfgPath)) FromConfig.props(props) else props
  }

  
  override def stopImpl(): Unit = {
  //  Await.ready(system.terminate(), Duration.Inf)
  }
   
}
