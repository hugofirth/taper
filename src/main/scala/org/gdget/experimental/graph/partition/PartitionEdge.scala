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
  def apply(toBeWrapped: Edge, globalId: Identifier, parent: Partition) = new PartitionEdge(toBeWrapped, globalId, parent)
}

/** Description of Class
  *
  * @author hugofirth
  */
class PartitionEdge private(wrapped: Edge, globalId: Identifier, parent: Partition) extends Edge {

  //Set globalId property to provided value - convert to Long because some Graph vendors limit property types
  wrapped.setProperty("__globalId", globalId.toLong)

  override def getVertex(direction: Direction): Vertex = {
    val v = wrapped.getVertex(direction)
    PartitionVertex(v, v.getProperty[Identifier]("__globalId"), parent)
  }

  override def getLabel: String = wrapped.getLabel

  override def getProperty[T](propertyKey: String): T = wrapped.getProperty[T](propertyKey)

  override def getId: java.lang.Long = wrapped.getProperty[Identifier]("__globalId")

  override def setProperty(propertyKey: String, propertyValue: scala.Any): Unit =
    wrapped.setProperty(propertyKey, propertyValue)

  override def getPropertyKeys: java.util.Set[String] = wrapped.getPropertyKeys

  override def remove(): Unit = wrapped.remove()

  override def removeProperty[T](propertyKey: String): T = wrapped.removeProperty[T](propertyKey)
}
