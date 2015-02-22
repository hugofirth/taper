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

import org.gdget.collection.{MutableTrieMultiMap, MutableTrieMap}

import scala.collection.mutable

/** Description of Class
  *
  * @author hugofirth
  */
class TraversalPatternSummary(queryFrequencies: Map[String, Int], maxPathLength: Int) {

  type VertexLabel = String
  type Query = String

  val trie = new MutableTrieMap[VertexLabel, mutable.Set[Query]] with MutableTrieMultiMap[VertexLabel, Query]
  val querySet = queryFrequencies.keySet
  val pathLength = maxPathLength

  lazy val labelFrequencies: MutableTrieMap[VertexLabel, Map[Query, Float]] = {
    def constructFrequencyTrie(subTrie: MutableTrieMap[VertexLabel, mutable.Set[Query]],
                               freqTrie: MutableTrieMap[VertexLabel, Map[Query, Float]],
                               label: Seq[VertexLabel] = Seq.empty[VertexLabel]): MutableTrieMap[VertexLabel, Map[Query, Float]]  = {
      //Potentially unsafe retrieval of Option
      val parentFrequencies = freqTrie.withPrefix(label).value.getOrElse {
        throw new IllegalArgumentException("TrieMap of frequency values should never have a value of None, even for the " +
          "root node.")
      }
      val counts = subTrie.suffixes.values.flatMap(_.value).flatten.foldLeft(Map.empty[Query, Int].withDefaultValue(0)) {
        case (m, x) => m + (x -> (1 + m(x)))
      }
      val childQueryFrequencies = counts.map { case (k, v) => (k, parentFrequencies.getOrElse(k, 0F) / v)}

      subTrie.suffixes.foreach { case (l, subSubTrie) =>
        freqTrie += ((label :+ l) -> subSubTrie.value.getOrElse(Set.empty[VertexLabel]).map(q => (q, childQueryFrequencies(q))).toMap)
        constructFrequencyTrie(subTrie.withPrefix(Seq(l)), freqTrie, label :+ l)
      }

      freqTrie
    }

    val frequencies = queryFrequencies.map { case (k,v) => (k, v.toFloat) }
    val freqs = MutableTrieMap.empty[VertexLabel, Map[Query, Float]]
    //Set root node of trie
    freqs += Seq.empty[VertexLabel] -> frequencies
    constructFrequencyTrie(trie, freqs)
  }

  //TODO: Make sure this is doing what we think and returning probs for all labels. It should be - but check.
  def getLikelihoodsFromPattern(pathLabels: Seq[VertexLabel]): Map[VertexLabel, Float] = {
    val freqTrie = labelFrequencies.withPrefix(pathLabels)
    freqTrie.value.map(_.values.sum) match {
      case Some(total) if total>0 =>
        freqTrie.suffixes.map { case (label, subTrie) =>
          (label, subTrie.value.getOrElse(Map.empty[VertexLabel, Float]).values.sum/total)
        }
      case _ => Map.empty[VertexLabel, Float]
    }
  }

  def contains(pattern: Seq[VertexLabel]): Boolean = trie.contains(pattern)
}

object TraversalPatternSummary {
  def apply(queryFrequencies: Map[String, Int], maxPathLengh: Int) = new TraversalPatternSummary(queryFrequencies, maxPathLengh)
}
