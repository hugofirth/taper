/** contextual-stability
  *
  * Copyright (c) 2014 Hugo Firth
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

/** Opt is a wrapper class for Option to allow for a cleaner API surface.
  *
  * @author hugofirth
  */
class Opt[T] private (val option: Option[T])

object Opt {
  implicit def any2opt[T](t: T): Opt[T] = new Opt(Option(t)) // NOT Some(t)
  implicit def option2opt[T](o: Option[T]): Opt[T] = new Opt(o)
  implicit def opt2option[T](o: Opt[T]): Option[T] = o.option
}
