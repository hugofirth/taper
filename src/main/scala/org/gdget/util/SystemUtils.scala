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
package org.gdget.util

/** Description of Class
  *
  * @author hugofirth
  */
object SystemUtils {
  def heapStats: String = {
    val mb = 1024*1024

    //Getting the runtime reference from system
    val runtime = Runtime.getRuntime

    "##### Heap utilization statistics #####" + sys.props("line.separator") + "Used Memory: " +
      ((runtime.totalMemory() - runtime.freeMemory()) / mb) + "MB" + sys.props("line.separator") +
      "Free Memory: "+ (runtime.freeMemory() / mb) + "MB" + sys.props("line.separator") + "Total Memory: " +
      (runtime.totalMemory() / mb) + "MB" + sys.props("line.separator") + "Max Memory: " + (runtime.maxMemory() / mb) + "MB"
  }
}
