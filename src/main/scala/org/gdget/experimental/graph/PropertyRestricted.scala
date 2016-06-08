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
package org.gdget.experimental.graph

import com.tinkerpop.blueprints.{Vertex, Edge, Element, Graph => BlueprintsGraph}

/** Description of Class
  *
  * @author hugofirth
  */
trait PropertyRestrictedGraph extends Graph {

  def validKeys: Set[String]

  override protected def edgeFactory: (Any, Vertex, Vertex, EdgeLabel) => BlueprintsGraph => Edge with PropertyRestrictedElement

  override protected def vertexFactory: (Any) => BlueprintsGraph => Vertex with PropertyRestrictedElement
}

trait PropertyRestrictedElement extends Element {

  protected var properties = Map.empty[String, Any]

  protected def graph: PropertyRestrictedGraph

  override def setProperty(key: String, value: Any) =
    if(graph.validKeys contains key) properties = properties + ((key, value))
}
