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
import com.netflix.atlas.core.db.Database

+import akka.cluster.sharding.ShardRegion
+import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}

object LocalDatabaseActor{
  import com.netflix.atlas.webapi.PublishApi._

  def name = "LocalDatabaseActor"

  def extractShardId: ExtractShardId = {
    case PublishRequest(_, _) =>
      this.toString
  }

  def extractEntityId: ExtractEntityId = {
    case msg @ PublishRequest(_, _) =>
      (this.toString, msg)
  }
}


class LocalDatabaseActor(db: Database) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.GraphApi._
  import com.netflix.atlas.webapi.TagsApi._

  def receive = {
    case ListTagsRequest(tq)    => sender() ! TagListResponse(db.index.findTags(tq))
    case ListKeysRequest(tq)    => sender() ! KeyListResponse(db.index.findKeys(tq).map(_.name))
    case ListValuesRequest(tq)  => sender() ! ValueListResponse(db.index.findValues(tq))
    case req: DataRequest       => sender() ! executeDataRequest(req)
  }

  private def executeDataRequest(req: DataRequest): DataResponse = {
    val data = req.exprs.map(expr => expr -> db.execute(req.context, expr)).toMap
    DataResponse(data)
  }
}

