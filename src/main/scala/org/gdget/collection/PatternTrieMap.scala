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
package org.gdget.collection

import scala.collection.immutable

/** Description of Class
  *
  * @author hugofirth
  */
class PatternTrieMap[A,B](kv:(Seq[A],B)*) extends Map[Seq[A],B] with immutable.MapLike[Seq[A],B, PatternTrieMap[A, B]] {

  val suffixes: Map[A, PatternTrieMap[A, B]] = Map.empty

  override def +[C >: B](kv: (Seq[A], C)): Map[Seq[A], C] = ???

  override def get(key: Seq[A]): Option[B] = ???

  override def iterator: Iterator[(Seq[A], B)] = ???

  override def -(key: Seq[A]): PatternTrieMap[A, B] = ???

  override def empty = new PatternTrieMap[A,B]
}
