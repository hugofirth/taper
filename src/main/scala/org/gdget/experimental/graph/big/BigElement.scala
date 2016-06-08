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

import java.util.{Set => JSet}

import scala.collection.JavaConverters._

import org.gdget.util.Identifier
import org.gdget.experimental.graph.PropertyRestrictedElement

import com.tinkerpop.blueprints.{Element, Graph => BlueprintsGraph}

/** Description of Class
  *
  * @author hugofirth
  */
trait BigElement extends Element with PropertyRestrictedElement {
  override protected def graph: BigGraph
  protected def id: Long

  override def getProperty[T](key: String): T = properties.get(key).orNull.asInstanceOf[T]

  override def getId: Identifier = id

  override def setProperty(key: String, value: Any): Unit = {
    if(properties.keySet.size>=4)
      throw new UnsupportedOperationException("BigGraph elements must have fewer than 5 entries")
    else super.setProperty(key, value)
  }

  override def getPropertyKeys: JSet[String] = properties.keySet.asJava

  override def remove(): Unit

  override def removeProperty[T](key: String): T = {
    val removed = getProperty[T](key)
    properties = properties - key
    removed
  }

  override def hashCode(): Int = id.hashCode

  def canEqual(other: Any): Boolean = other.isInstanceOf[BigElement]

  override def equals(other: Any): Boolean = other match {
    case that: BigElement => (that canEqual this) && id == that.id
    case _ => false
  }
}
