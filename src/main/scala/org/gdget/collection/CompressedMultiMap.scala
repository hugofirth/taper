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

import scala.collection.mutable

/** Effectively a straight copy and list of [[scala.collection.mutable.MultiMap]] which uses immutable Sets internally.
  * This takes advantage of the fact that immutable.Set uses a highly optimised representation for <4 elements.
  *
  * @author hugofirth
  */
trait CompressedMultiMap[A, B] extends mutable.Map[A, Set[B]] {


  /** Assigns the specified `value` to a specified `key`.  If the key
    *  already has a binding to equal to `value`, nothing is changed;
    *  otherwise a new binding is added for that `key`.
    *
    *  @param key    The key to which to bind the new value.
    *  @param value  The value to bind to the key.
    *  @return       A reference to this multimap.
    */
  def addBinding(key: A, value: B): this.type = {
    get(key) match {
      case None => this(key) = Set[B](value)
      case Some(set) => this(key) = set + value
    }
    this
  }

  /** Removes the binding of `value` to `key` if it exists, otherwise this
    *  operation doesn't have any effect.
    *
    *  If this was the last value assigned to the specified key, the
    *  set assigned to that key will be removed as well.
    *
    *  @param key     The key of the binding.
    *  @param value   The value to remove.
    *  @return        A reference to this multimap.
    */
  def removeBinding(key: A, value: B): this.type = {
    get(key) match {
      case None =>
      case Some(set) => if ((set - value).isEmpty) this -= key else this(key) = set - value
    }
    this
  }

  /** Checks if there exists a binding to `key` such that it satisfies the predicate `p`.
    *
    *  @param key   The key for which the predicate is checked.
    *  @param p     The predicate which a value assigned to the key must satisfy.
    *  @return      A boolean if such a binding exists
    */
  def entryExists(key: A, p: B => Boolean): Boolean = get(key) match {
    case None => false
    case Some(set) => set exists p
  }
}
