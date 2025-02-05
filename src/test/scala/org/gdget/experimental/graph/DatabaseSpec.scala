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

import com.tinkerpop.blueprints.{Graph => BlueprintsGraph}
import org.gdget.util.FileUtils
import org.scalatest.{Suite, BeforeAndAfterAll}


/** Description of Class
  *
  * @author hugofirth
  */
trait DatabaseSpec extends BeforeAndAfterAll { this: Suite =>

  private var databases: List[BlueprintsGraph] = List()
  private var clear: List[String] = List()
  def addDatabase(g: BlueprintsGraph*): Unit = databases ++= g
  def shouldClear(dir: String*): Unit = clear ++= dir

  override def afterAll(): Unit = {
    databases.foreach(_.shutdown())
    clear.map(FileUtils.removeAll)
  }

}
