/**
  * contextual-stability
  *
  * Copyright (c) 2014 Hugo Firth
  * Email: <me@hugofirth.com/>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at:
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.gdget.experimental.actors

import akka.actor.{Props, Actor}
import akka.event.Logging
import com.tinkerpop.blueprints.Vertex
import org.gdget.experimental.graph.partition.Partition

sealed trait PartitionActorMessage
case class AcceptOutcastVertex(o: Vertex, id: Long) extends PartitionActorMessage
case class RejectOutcastVertex(o: Vertex, id: Long) extends PartitionActorMessage
case class OfferOutcastVertex(o: Vertex, id: Long, score: Int) extends PartitionActorMessage


/**
  * Companion object for PartitionActor
  */
object PartitionActor {
  /**
    * Create Props for a PartitionActor actor.
    * @param partition the the partition to be loaded
    * @return a Props for creating this actor, which can then be further configured
    *         (e.g. calling `.withDispatcher()` on it)
    */
  def props(partition: Partition): Props = Props(new PartitionActor(partition))
}

/**
  * The PartitionActor class is responsible for communicating with other partitions in the distributed graph.
  * @param partition the Long id of the subGraph to load
  */
class PartitionActor(partition: Partition) extends Actor  {
  val log = Logging(context.system, this)

  override def receive = {
    case OfferOutcastVertex(o, id, score) => //Receive outcast vertex o from sender and work out if we want it
    case AcceptOutcastVertex(o, id) => //Tell sender we want outcast vertex o
    case RejectOutcastVertex(o, id) => //Tell sender we don't want outcast vertex o and remove it
    case _ => //Default case - throw error
  }

}
