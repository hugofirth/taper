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
package org.gdget.experimental.util.collection

import org.gdget.util.collection.{MutableTrieMultiMap, MutableTrieMap}

import scala.collection.mutable

import org.gdget.UnitSpec
import org.scalatest.{DoNotDiscover, Suites}

/** Description of Class
  *
  * @author hugofirth
  */
class TrieTestSuite extends Suites(new MutableTrieMapSpec, new TrieMultiMapSpec) {}

@DoNotDiscover
class MutableTrieMapSpec extends UnitSpec {

  var trie =  MutableTrieMap.empty[Char, Int]

  before {
    trie = MutableTrieMap[Char, Int]()
    trie += (("abd", 0))
    trie += (("acd", 1))
    trie += (("aca", 2))
    trie += (("cca", 3))
  }

  "A PatternTrieMap" should "be able to return all mappings for a given prefix" in {
    (trie withPrefix "a").toSet should equal (Set((Vector('b','d'), 0),
                                                  (Vector('c','d'), 1),
                                                  (Vector('c','a'), 2)))
  }

  it should "be able to remove a mapping" in {
    (trie -= "acd").toSet should equal (Set((Vector('a','b','d'), 0),
                                            (Vector('a','c','a'), 2),
                                            (Vector('c','c','a'), 3)))
  }

  it should "be able to add a mapping" in {
    (trie += (("cda", 4))).toSet should equal (Set((Vector('a','b','d'), 0),
                                                   (Vector('a','c','d'), 1),
                                                   (Vector('a','c','a'), 2),
                                                   (Vector('c','c','a'), 3),
                                                   (Vector('c','d','a'), 4)))
  }

  it should "be able to return the value associated with a given prefix" in { trie get "abd" map( _ should be (0) ) }

}

@DoNotDiscover
class TrieMultiMapSpec extends UnitSpec {

  var trie = new MutableTrieMap[Char, mutable.Set[Int]]() with MutableTrieMultiMap[Char, Int]

  before {
    trie = new MutableTrieMap[Char, mutable.Set[Int]]() with MutableTrieMultiMap[Char, Int]
    trie addBinding("abd", 0)
    trie addBinding("abd", 1)
    trie addBinding("acd", 2)
    trie addBinding("acda", 3)
  }

  "A PatternTrieMultiMap" should "be able to return all sets of mappings for a given prefix" in {
    (trie withPrefix "ab").toSet should equal (Set((Vector(), Set(0,1)),(Vector('d'), Set(0,1))))
  }

  it should "be able to remove an element of a set mapped to a given prefix" in {
    (trie removeBinding("abd", 0) withPrefix "abd").toSet should equal (Set((Vector(), Set(1))))
  }

  it should "be able to add an element to a set mapped to a given prefix" in {
    (trie addBinding("acd", 4) withPrefix "acd").toSet should equal (Set((Vector(), Set(2,3,4)),
                                                                         (Vector('a'), Set(3))))
  }

  it should "be able to confirm the existence of an element in a set mapped to a prefix" in {
    trie entryExists("abd", _ == 0) should be (true)
    trie entryExists("acda", _ == 3) should be (true)
    trie entryExists("a", _ == 2) should be (true)
    trie entryExists("abd", _ == -1) should be (false)
  }

}