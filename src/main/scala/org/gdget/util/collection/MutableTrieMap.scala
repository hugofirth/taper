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
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

/** Description of Class
  *
  * @author hugofirth
  */
class MutableTrieMap[A,B] extends mutable.Map[Seq[A],B] with mutable.MapLike[Seq[A],B, MutableTrieMap[A, B]] {

  var suffixes: Map[A, MutableTrieMap[A, B]] = Map.empty
  var value: Option[B] = None

  def withPrefix(prefix: Seq[A]): MutableTrieMap[A, B] = {
    if (prefix.isEmpty) {
      this
    } else {
      val leading = prefix.head
      suffixes get leading match {
        case None =>
          suffixes = suffixes + (leading -> empty)
        case _ =>
      }
      suffixes(leading) withPrefix prefix.tail
    }
  }

  @tailrec
  final def maximumMutualPrefix(remaining: Seq[A], prefix: Seq[A] = Seq.empty[A]): Seq[A] = suffixes get remaining.head match {
    case Some(subTrie) => subTrie.maximumMutualPrefix(remaining.tail, prefix:+remaining.head)
    case _ => prefix
  }

  override def get(key: Seq[A]): Option[B] = {
    if(key.isEmpty) { value } else { suffixes get key.head flatMap( _.get(key.tail) ) }
  }

  override def update(key: Seq[A], value: B): Unit = withPrefix(key).value = Some(value)

  override def remove(key: Seq[A]): Option[B] = {
    if(key.isEmpty) {
      val previous = value; value = None; previous
    } else {
      suffixes get key.head flatMap( _.remove(key.tail) )
    }
  }

  override def +=(kv: (Seq[A], B)): this.type = { update(kv._1, kv._2); this }

  override def -=(key: Seq[A]): this.type = { remove(key); this }

  override def iterator: Iterator[(Seq[A], B)] = {
    (for(m <- value.iterator) yield (IndexedSeq.empty[A], m)) ++
      (for {
        (symbol, subTries) <- suffixes.iterator
        (pattern, patterns) <- subTries.iterator
      } yield (symbol +: pattern, patterns))
  }

  override def empty = new MutableTrieMap[A, B]

}

object MutableTrieMap {

  def apply[A,B](mappings: (Seq[A],B)*): MutableTrieMap[A,B] = {
    val trie = new MutableTrieMap[A,B]
    mappings foreach( trie += _ )
    trie
  }

  def empty[A, B] = new MutableTrieMap[A, B]

  def newBuilder[A, B]: mutable.Builder[(Seq[A],B), MutableTrieMap[A, B]] =
    new mutable.MapBuilder[Seq[A], B, MutableTrieMap[A, B]](empty)

  implicit def canBuildFrom[A, B]: CanBuildFrom[MutableTrieMap[_,_], (Seq[A], B), MutableTrieMap[A, B]] = {
    new CanBuildFrom[MutableTrieMap[_,_], (Seq[A], B), MutableTrieMap[A, B]] {
      def apply(from: MutableTrieMap[_,_]) = newBuilder[A, B]
      def apply() = newBuilder[A, B]
    }
  }
}

