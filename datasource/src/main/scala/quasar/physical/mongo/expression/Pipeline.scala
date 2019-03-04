/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.mongo.expression

import slamdata.Predef._

import matryoshka.Delay

import quasar.{RenderTree, NonTerminal}, RenderTree.ops._
import quasar.physical.mongo.Version

trait Pipeline[+A] extends Product with Serializable

trait MongoPipeline[+A] extends Pipeline[A]

trait CustomPipeline extends Pipeline[Nothing]

import scalaz.Functor

object Pipeline {
  final case class $project[A](obj: Map[String, A]) extends MongoPipeline[A]
  final case class $match[A](obj: Map[String, A]) extends MongoPipeline[A]
  final case class $unwind[A](path: String, arrayIndex: String) extends MongoPipeline[A]

  implicit val delayRenderTreeMongoPipeline: Delay[RenderTree, Pipeline] =
    new Delay[RenderTree, Pipeline] {
      def apply[A](fa: RenderTree[A]): RenderTree[Pipeline[A]] = RenderTree.make {
        case $project(obj) => NonTerminal(List("$project"), None, obj.toList map {
          case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
        })
        case $match(obj) => NonTerminal(List("$match"), None, obj.toList map {
          case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
        })
        case $unwind(p, a) =>
          NonTerminal(List("$unwind"), None, List(p.render, a.render))
        case NotNull(fld) =>
          NonTerminal(List("NotNull"), None, List(fld.render))
      }
  }

  final case class NotNull(field: String) extends CustomPipeline

  def pipeMinVersion(pipe: MongoPipeline[_]): Version = pipe match {
    case $project(_) => Version.zero
    case $match(_) => Version.zero
    case $unwind(_, _) => Version.$unwind
  }

  implicit val functorPipeline: Functor[Pipeline] = new Functor[Pipeline] {
    def map[A, B](fa: Pipeline[A])(f: A => B): Pipeline[B] = fa match {
      case NotNull(fld) => NotNull(fld)
      case $project(obj) => $project(obj map { case (k, v) => (k, f(v)) })
      case $match(obj) => $match(obj map { case (k, v) => (k, f(v)) })
      case $unwind(p, i) => $unwind(p, i)
    }
  }
}
