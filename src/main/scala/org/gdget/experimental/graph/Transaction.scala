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

import com.tinkerpop.blueprints.{Graph => BlueprintsGraph, TransactionalGraph}

/** Description of Class
  *
  * @author hugofirth
  */
sealed trait Transaction {
  def commit(): Unit
  def rollback(): Unit
}

case class GenericTransaction(txG: TransactionalGraph) extends Transaction {
  override def commit(): Unit = txG.commit()

  override def rollback(): Unit = txG.rollback()
}

object Transaction {

  def forGraph(graph: BlueprintsGraph): Option[Transaction] = graph match {
    case g: TransactionalGraph => Some(GenericTransaction(g))
    case g: BlueprintsGraph => None
  }

  def supportedByGraph(graph: BlueprintsGraph): Boolean = graph.isInstanceOf[TransactionalGraph]

}
