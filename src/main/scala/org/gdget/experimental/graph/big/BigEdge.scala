/** contextual-stability
  *
  * Copyright (c) 2015 Hugo Firth
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
package org.gdget.experimental.graph.big

import com.tinkerpop.blueprints.util.{StringFactory, ExceptionFactory}
import com.tinkerpop.blueprints.{Direction, Edge, Graph => BlueprintsGraph, Vertex}

/** Description of Class
  *
  * @author hugofirth
  */
case class BigEdge(id: Long, outVertex: Vertex, inVertex: Vertex, label: String, private val parent: BlueprintsGraph)
  extends Edge with BigElement {

  //TODO: Get rid of this horror in refactor
  require(parent.isInstanceOf[BigGraph], "BigEdge parent graph must be of type BigGraph. Found " +
    parent.getClass.getCanonicalName)

  override protected val graph = parent.asInstanceOf[BigGraph]

  override def getLabel: String = label

  override def getVertex(direction: Direction): Vertex = direction match {
    case Direction.OUT => outVertex
    case Direction.IN =>  inVertex
    case Direction.BOTH => throw ExceptionFactory.bothIsNotSupported()
  }

  override def toString: String = StringFactory.edgeString(this)

  override def remove(): Unit = graph.removeEdge(this)
}
