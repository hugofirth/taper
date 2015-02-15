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

import org.gdget.UnitSpec
import org.scalatest.{DoNotDiscover, Suites}

/** Description of Class
  *
  * @author hugofirth
  */
class TraversalPatternSummaryTestSuite extends Suites(new TraversalPatternSummarySpec) {}

@DoNotDiscover
class TraversalPatternSummarySpec extends UnitSpec {

  val frequencies = Map("Q1" -> 4, "Q2" -> 4)
  var summary: TraversalPatternSummary = _

  before {
    summary = TraversalPatternSummary(frequencies, 3)
    summary.trie addBinding(Seq("a","b","d"), "Q1")
    summary.trie addBinding(Seq("a","c","d"), "Q1")
    summary.trie addBinding(Seq("a","c","a"), "Q2")
    summary.trie addBinding(Seq("c","c","a"), "Q2")
  }

  "A TraversalPatternSummary" should "be able to return the likelihoods of labels as the next step in a path" in {
    summary.getLikelihoodsFromPattern(Seq.empty[String]) should equal (Map("a" -> 3F/4, "c" -> 1F/4))
    summary.getLikelihoodsFromPattern(Seq("a")) should equal (Map("b" -> 1F/3, "c" -> 2F/3))
    summary.getLikelihoodsFromPattern(Seq("a", "b", "d")) should equal (Map())
  }

}
