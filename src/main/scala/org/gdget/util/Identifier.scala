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
package org.gdget.util

import org.gdget.experimental.graph.UnsupportedIdFormatException

/** Description of Class
  *
  * @author hugofirth
  */
class Identifier private (val id: Long) extends Ordered[Identifier] {

  override def toString: String = "id:"+id.toString

  def canEqual(other: Any): Boolean = other.isInstanceOf[Identifier]

  override def equals(other: Any): Boolean = other match {
    case that: Identifier =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def compare(that: Identifier): Int = this.id compare that.id
}

object Identifier {
  implicit def string2Id(s: String): Identifier = new Identifier(s.toLong)
  implicit def id2String(id: Identifier): String = id.id.toString
  implicit def long2Id(l: Long): Identifier = new Identifier(l)
  implicit def id2Long(id: Identifier): Long = id.id
  implicit def javaLong2Id(jl: java.lang.Long): Identifier = new Identifier(jl.longValue)
  implicit def id2JavaLong(id: Identifier): java.lang.Long = java.lang.Long.valueOf(id.id)
  implicit def any2Id(o: Any): Identifier = o match {
    case s: String => string2Id(s)
    case l: Long => long2Id(l)
    case default => throw UnsupportedIdFormatException(Some(default))
  }
}
