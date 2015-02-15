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
package org.gdget.util.collection

import scala.annotation.tailrec
import scala.collection.mutable

/** Description of Class
  *
  * @author hugofirth
  */
trait MutableTrieMultiMap[A, B] extends MutableTrieMap[A, mutable.Set[B]] with mutable.MultiMap[Seq[A], B] {

  override final def addBinding(key: Seq[A], valueElem: B): this.type = { recursiveAddBinding(this, key, valueElem); this }

  @tailrec
  private def recursiveAddBinding(trieMultiMap: MutableTrieMap[A,mutable.Set[B]], key: Seq[A], valueElem: B): Unit = {
    trieMultiMap.value = trieMultiMap.value match {
      case Some(set) => Some(set += valueElem)
      case None => Some(mutable.Set(valueElem))
    }
    if(key.nonEmpty) { recursiveAddBinding(trieMultiMap.withPrefix(key take 1), key.tail, valueElem) }
  }

  override final def removeBinding(key: Seq[A], valueElem: B): this.type = { recursiveRemoveBinding(this, key, valueElem); this }

  @tailrec
  private def recursiveRemoveBinding(trieMultiMap: MutableTrieMap[A,mutable.Set[B]], key: Seq[A], valueElem: B): Unit = {
    trieMultiMap.value.map( _.remove(valueElem) )
    if(key.nonEmpty) { recursiveRemoveBinding(trieMultiMap.withPrefix(key take 1), key.tail, valueElem) }
  }

  override def empty =  new MutableTrieMap[A, mutable.Set[B]] with MutableTrieMultiMap[A, B]

}
