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
package com.netflix.atlas.standalone

import java.io.File

import akka.actor.ActorSystem
import com.google.inject.AbstractModule
import com.google.inject.Module
import com.google.inject.multibindings.Multibinder
import com.google.inject.multibindings.OptionalBinder
import com.google.inject.util.Modules
import com.netflix.atlas.akka.ActorService
import com.netflix.atlas.akkacluster.ActorClusterService
import com.netflix.atlas.akka.WebServer
import com.netflix.atlas.config.ConfigManager
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.webapi.DatabaseProvider
import com.netflix.iep.guice.GuiceHelper
import com.netflix.iep.service.Service
import com.netflix.iep.service.ServiceManager
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Spectator
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import com.netflix.spectator.servo.ServoRegistry;
import com.netflix.spectator.api.Clock;
import com.netflix.servo.publish.CounterToRateMetricTransform;
import com.netflix.servo.publish.FileMetricObserver;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.util.concurrent.TimeUnit;
import com.netflix.servo.publish.PollScheduler;
import com.netflix.servo.publish.PollRunnable;
import com.netflix.servo.publish.BasicMetricFilter;
import com.netflix.servo.publish.MonitorRegistryMetricPoller;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.publish.AsyncMetricObserver;

import com.netflix.servo.publish.atlas.AtlasMetricObserver
import com.netflix.servo.publish.atlas.ServoAtlasConfig
import java.util.HashMap;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.TagList;
import java.net.InetAddress;
import java.net.UnknownHostException;
import com.netflix.servo.publish.MetricPoller;
import com.netflix.spectator.gc.GcLogger;
import com.netflix.spectator.jvm.Jmx;

// chill
//import com.twitter.chill

/**
 * Provides a simple way to start up a standalone server. Usage:
 *
 * ```
 * $ java -jar atlas.jar config1.conf config2.conf
 * ```
 */


object Main extends StrictLogging {

  private var guice: GuiceHelper = _
  
//  def MetricObserver createFileObserver(File dir): Unit = {
//    if (!dir.mkdirs() && !dir.isDirectory())
//      throw new IllegalStateException("failed to create metrics directory: " + dir);
//    return rateTransform(new FileMetricObserver("servo-example", dir));
//  }

  
  private def loadAdditionalConfigFiles(files: Array[String]): Unit = {
    files.foreach { f =>
      logger.info(s"loading config file: $f")
      val c = ConfigFactory.parseFileAnySyntax(new File(f))
      ConfigManager.update(c)
    }
  }

  private def rateTransform(observer: MetricObserver): MetricObserver = {
    val heartbeat = 2 * 60;
    return new CounterToRateMetricTransform(observer, heartbeat, TimeUnit.SECONDS)
  }
  
  private def async(name: String, observer: MetricObserver): MetricObserver = {
    val expireTime = 2000 * 60  // 2 seconds * poll interval
    val queueSize = 1000 // roughly 200 metrics are collected
    //return new AsyncMetricObserver(name, observer, queueSize, expireTime);
    // unbounded
    return new AsyncMetricObserver(name, observer);
  }
  
  private def createFileObserver(dir: File): MetricObserver = {
    if (!dir.mkdirs() && !dir.isDirectory())
      throw new IllegalStateException("failed to create metrics directory: " + dir);
    return rateTransform(new FileMetricObserver("atlas", dir));
  }
  
  private def getCommonTags(): TagList = {
    var tags = new HashMap[String,String]()
    var cluster = System.getenv("NETFLIX_CLUSTER")
    if (cluster == null) cluster = "unknown_cluster"
    logger.info("cluster", cluster)
    tags.put("nf.cluster", cluster)
    logger.info("commontags", tags.toString())
    try {
      tags.put("nf.node", InetAddress.getLocalHost().getHostName())
    } catch {
        case e: UnknownHostException =>
          tags.put("nf.node", "unknown")
    }
    logger.info("commontags", tags.toString())
    val ctags = BasicTagList.copyOf(tags)
    logger.info("commontags ", ctags.toString())

    return BasicTagList.copyOf(tags);
  }

  private def isAtlasObserverEnabled(): Boolean = {
    var isEnabled = false
    try {
      val config = ConfigManager.current.getConfig("atlas.servo")
      isEnabled = config.getBoolean("atlasObserverEnabled")
    } catch {
        case e: Exception =>
          isEnabled = false
    }
    logger.info("Servo Atlas Observer Enabled: {}", isEnabled)
    return isEnabled
  }
  
  private def getAtlasObserverUri(): String = {
    var uri = "http://localhost:7101/api/v1/publish"
    try {
      val config = ConfigManager.current.getConfig("atlas.servo")
      uri = config.getString("atlasObserverUri")
    } catch {
        case e: Exception =>
          uri = "http://localhost:7101/api/v1/publish"
    }
    logger.info("Servo Atlas Observer URI: {}", uri)
    return uri
  }
  
  private def isJvmExtEnabled(): Boolean = {
    var isEnabled = false
    try {
      val config = ConfigManager.current.getConfig("atlas.servo")
      isEnabled = config.getBoolean("jvmExtEnabled")
    } catch {
        case e: Exception =>
          isEnabled = false
    }
    logger.info("Servo JVM Extension Enabled: {}", isEnabled)
    return isEnabled
  }

  private def isGcExtEnabled(): Boolean = {
    var isEnabled = false
    try {
      val config = ConfigManager.current.getConfig("atlas.servo")
      isEnabled = config.getBoolean("gcExtEnabled")
    } catch {
        case e: Exception =>
          isEnabled = false
    }
    logger.info("Servo Garbage Collection Extension Enabled: {}", isEnabled)
    return isEnabled
  }
  
  private def isFileObserverEnabled(): Boolean = {
    var isEnabled = false
    try {
      val config = ConfigManager.current.getConfig("atlas.servo")
      isEnabled = config.getBoolean("fileObserverEnabled")
    } catch {
        case e: Exception =>
          isEnabled = false
    }
    logger.info("Servo File Observer Extension Enabled: {}", isEnabled)
    return isEnabled
  }
  
  private def getFileObserverDirectory(): String = {
    var aDir = "servo-out"
    try {
      val config = ConfigManager.current.getConfig("atlas.servo")
      aDir = config.getString("fileObserverDirectory")
    } catch {
        case e: Exception =>
          aDir = "servo-out"
    }
    logger.info("Servo File Observer Directory: {}", aDir)
    return aDir
  }
  
  private def getAtlasConfig(): ServoAtlasConfig = {
    return new ServoAtlasConfig() {
      @Override
      def getAtlasUri(): String = {
        return getAtlasObserverUri();
      }

      @Override
      def getPushQueueSize(): Int = {
        return 1000;
      }

      @Override
      def shouldSendMetrics(): Boolean = {
        return isAtlasObserverEnabled();
      }

      @Override
      def batchSize(): Int = {
        return 10000;
      }
    };
  }
  private def createAtlasObserver(): MetricObserver = {
    val cfg = getAtlasConfig()
    val common = getCommonTags()
    //var amo = new AtlasMetricObserver(cfg, common);
    //var x = async("atlas", amo)
    //var rt = rateTransform(x)
    //return rt
    return rateTransform(async("atlas", new AtlasMetricObserver(cfg, common)))
  }
  
  private def schedule(poller: MetricPoller, observers: List[MetricObserver]): Unit = {
    var task = new PollRunnable(poller, BasicMetricFilter.MATCH_ALL, true, observers)
    PollScheduler.getInstance().addPoller(task, 60, TimeUnit.SECONDS)
  }
  
  private def initMetricsExtensions(): Unit = {
    if (isGcExtEnabled()) {
      // LOGGER.info("garbage collection extension enabled")
      var GC_LOGGER = new GcLogger()
      GC_LOGGER.start(null)
    }
    if (isJvmExtEnabled()) {
      //LOGGER.info("jvm extension enabled")
      Jmx.registerStandardMXBeans(Spectator.globalRegistry())
    }
  }
  
  private def initMetricsPublishing(): Unit = {
    var observers = new ArrayList[MetricObserver]()

    if (isFileObserverEnabled()) {
      var dir = new File(getFileObserverDirectory())
      //LOGGER.info("file observer enabled, logging to: " + dir)
      observers.add(createFileObserver(dir))
    }

    if (isAtlasObserverEnabled()) {
      var uri = getAtlasObserverUri()
      logger.info("URI = {}", uri)
      //LOGGER.info("atlas observer enabled, uri: " + uri)
      var ao = createAtlasObserver()
      observers.add(ao)
    }

    // only start if there are observers
    if (observers.size > 0) {
      PollScheduler.getInstance().start()
      schedule(new MonitorRegistryMetricPoller(), observers)
      var r = new ServoRegistry()
      Spectator.globalRegistry().add(r)
    }
  }
  
  def main(args: Array[String]): Unit = {
    try {
      loadAdditionalConfigFiles(args)
      start()
      guice.addShutdownHook()
    } catch {
      case t: Throwable =>
        logger.error("server failed to start, shutting down", t)
        System.exit(1)
    }
  }

  def start(): Unit = {

    val configModule = new AbstractModule {
      override def configure(): Unit = {
        bind(classOf[Config]).toInstance(ConfigManager.current)
        bind(classOf[Registry]).toInstance(Spectator.globalRegistry())
        initMetricsExtensions()
        initMetricsPublishing()       
      }
      
      private def providesRegistry(): Registry = {
        return new ServoRegistry()
      }
    }

    val modules = GuiceHelper.getModulesUsingServiceLoader
    modules.add(configModule)

    //var globalReg = Spectator.globalRegistry();
    //var r = new ServoRegistry();
    //Spectator.globalRegistry().add(r);

    guice = new GuiceHelper
    guice.start(modules)

    // Ensure that service manager instance has been created
    guice.getInjector.getInstance(classOf[ServiceManager])
  }

  def shutdown(): Unit = guice.shutdown()
}
