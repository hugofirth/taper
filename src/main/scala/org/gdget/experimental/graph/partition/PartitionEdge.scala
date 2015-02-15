/** contextual-stability
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
package org.gdget.experimental.graph.partition

import com.tinkerpop.blueprints.{Vertex, Direction, Edge}
import org.gdget.util.Identifier

object PartitionEdge {
  def apply(toBeWrapped: Edge, globalId: Identifier, partition: Partition) = new PartitionEdge(toBeWrapped, globalId, partition)
  def unapply(edge: PartitionEdge): Option[Edge] = Some(edge.wrapped)
  def unwrap(e: Edge): Edge = e match {
    case PartitionEdge(wrapped) => wrapped
    case _ => e
  }
}

/** Description of Class
  *
  * @author hugofirth
  */
class PartitionEdge private(val wrapped: Edge, globalId: Identifier, val partition: Partition) extends Edge {

  //Set globalId property to provided value - convert to Long because some Graph vendors limit property types
  wrapped.setProperty("__globalId", globalId.toLong)

  //Convenience fields as Edge vertices should never change anyway
  lazy val in = getVertex(Direction.IN)
  lazy val out = getVertex(Direction.OUT)

  override def getVertex(direction: Direction): PartitionVertex = {
    val v = wrapped.getVertex(direction)
    PartitionVertex(v, v.getProperty[Any]("__globalId"), partition)
  }

  override def getLabel: String = wrapped.getLabel

  override def getProperty[T](propertyKey: String): T = wrapped.getProperty[T](propertyKey)

  override def getId: Identifier = wrapped.getProperty[Any]("__globalId")

  override def setProperty(propertyKey: String, propertyValue: scala.Any): Unit =
    wrapped.setProperty(propertyKey, propertyValue)

  override def getPropertyKeys: java.util.Set[String] = wrapped.getPropertyKeys

  override def remove(): Unit = wrapped.remove()

  override def removeProperty[T](propertyKey: String): T = wrapped.removeProperty[T](propertyKey)

  override def equals(other: Any): Boolean = other match {
    case that: Edge =>
      other.isInstanceOf[Edge] &&
        this.getId == that.getId
    case _ => false
  }

  override def hashCode(): Int = this.getId.hashCode()
}
