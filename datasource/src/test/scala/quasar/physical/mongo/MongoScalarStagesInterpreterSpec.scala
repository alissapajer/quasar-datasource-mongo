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

package quasar.physical.mongo

import slamdata.Predef._

import org.bson.{Document => _, _}

import quasar.common.CPath
import quasar.{IdStatus, ScalarStageSpec => Spec, ScalarStage, ScalarStages}

class MongoScalarStagesInterpreterSpec
    extends Spec.WrapSpec
    with Spec.ProjectSpec
    with Spec.MaskSpec
    with Spec.PivotSpec
    with Spec.FocusedSpec
    with Spec.CartesianSpec
    with StageInterpreterSpec {
  val wrapPendingExamples: Set[Int] = Set()
  val projectPendingExamples: Set[Int] = Set()
  val maskPendingExamples: Set[Int] = Set()
  val pivotPendingExamples: Set[Int] = Set()
  val focusedPendingExamples: Set[Int] = Set(41, 44)
  val cartesianPendingExamples: Set[Int] = Set()

  "Id statuses" >> {
    val input = ldjson("""
      {"_id": "0", "value": "foo"}
      {"_id": "1", "value": "bar"}
      {"_id": "2", "value": "baz"}""")
    "ExcludeId" >> {
      val actual = interpret(ScalarStages(IdStatus.ExcludeId, List()), input, (x => x))
      actual must bestSemanticEqual(input)
    }
    "IdOnly" >> {
      val expected = ldjson("""
        "0"
        "1"
        "2"""")
      val actual = interpret(ScalarStages(IdStatus.IdOnly, List()), input, (x => x))
      actual must bestSemanticEqual(expected)
    }
    "IncludeId" >> {
      val expected = ldjson("""
        ["0", {"_id": "0", "value": "foo"}]
        ["1", {"_id": "1", "value": "bar"}]
        ["2", {"_id": "2", "value": "baz"}]""")
      val actual = interpret(ScalarStages(IdStatus.IncludeId, List()), input, (x => x))
      actual must bestSemanticEqual(expected)
    }
  }

//InterpretedRead(ResourcePath(/datasource/8a0604f2-5f3d-4ccc-97c7-ab36c049074b/test/nested.data))
//╰─ ScalarStages(ExcludeId)
//   ├─ Project(.topObj)
//   ├─ Mask
//   │  ╰─ Column[.](Object)
//   ├─ Pivot(IncludeId, Object)
//   ├─ Mask
//   │  ├─ Column[[0]](⊤)
//   │  ╰─ Column[[1]](⊤)
//   ├─ Mask
//   │  ├─ Column[[0]](⊤)
//   │  ╰─ Column[[1]](Object)
//   ├─ Wrap(cartesian)
//
  "foobar" >> {
    import quasar.api.table.ColumnType
    import quasar.common.CPathField

    "testtest" >> {
      val input = ldjson("""{"topObj": {"midArr":[[10, 11, 12], {"a": "j", "b": "k", "c": "l"}], "midObj": {"botArr":[13, 14, 15], "botObj": {"a": "m", "b": "n", "c": "o"}}}}""")
      val stages = List(
        Project(CPath.parse(".topObj")),
        Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
        Pivot(IdStatus.IncludeId, ColumnType.Object),
        Mask(Map(
          CPath.parse("[0]") -> ColumnType.Top,
          CPath.parse("[1]") -> ColumnType.Top)),
        Mask(Map(
          CPath.parse("[0]") -> ColumnType.Top,
          CPath.parse("[1]") -> Set(ColumnType.Object))),
        Wrap("cartesian"))

      val expected = ldjson("""
        { "cartesian": ["midArr"] }
        { "cartesian": ["midObj", {"botArr":[13, 14, 15], "botObj": {"a": "m", "b": "n", "c": "o"}}] }
      """)

 //   ├─ Cartesian
 //   │  ├─ Key(".cartouche0")
 //   │  │  ╰─ tuple
 //   │  │     ├─ String(".cartesian")
 //   │  │     ╰─ List
 //   │  │        ╰─ Project([0])
 //   │  ╰─ Key(".cartouche1")
 //   │     ╰─ tuple
 //   │        ├─ String(".cartesian")
 //   │        ╰─ List
 //   │           ├─ Project([1])
 //   │           ├─ Pivot(IncludeId, Object)
 //   │           ╰─ Mask
 //   │              ├─ Column[[0]](⊤)
 //   │              ╰─ Column[[1]](⊤)

      val expected2 = ldjson("""
        { "cartouche0": "midArr" }
        { "cartouche0": "midObj", "cartouche1": ["botArr", [13, 14, 15]] }
        { "cartouche0": "midObj", "cartouche1": ["botObj", {"a": "m", "b": "n", "c": "o"}] }
      """)

      val expected3 = ldjson("""
        { "cartouche0": "midArr" }
        { "cartouche0": "midObj", "cartouche1": {"botArr":[13, 14, 15], "botObj": {"a": "m", "b": "n", "c": "o"}} }
      """)

      val expected4 = ldjson("""
        { "x": ["foo"] }
        { "x": ["bar", 42] }
      """)

      val expected5 = ldjson("""
        { "x0": "foo" }
        { "x0": "bar", "x1": 42 }
      """)
      val targets0 = Map(
        (CPathField("cartouche0"), (CPathField("cartesian"), List(
          Project(CPath.parse("[0]"))))),
        (CPathField("cartouche1"), (CPathField("cartesian"), List(
          Project(CPath.parse("[1]")),
          Pivot(IdStatus.IncludeId, ColumnType.Object),
          Mask(Map(
            CPath.parse("[0]") -> ColumnType.Top,
            CPath.parse("[1]") -> ColumnType.Top))))))

      val targets1 = Map(
        (CPathField("cartouche0"), (CPathField("cartesian"), List(
          Project(CPath.parse("[0]"))))),
        (CPathField("cartouche1"), (CPathField("cartesian"), List(
          Project(CPath.parse("[1]")),
          Pivot(IdStatus.IncludeId, ColumnType.Object)))))

      val targets2 = Map(
        (CPathField("cartouche0"), (CPathField("cartesian"), List(
          Project(CPath.parse("[0]"))))),
        (CPathField("cartouche1"), (CPathField("cartesian"), List(
          Project(CPath.parse("[1]"))))))

      val targets = Map(
        (CPathField("x0"), (CPathField("x"), List(
          Project(CPath.parse("[0]"))))),
        (CPathField("x1"), (CPathField("x"), List(
          Project(CPath.parse("[1]"))))))

      //input must interpretInto(stages)(expected)
      expected4 must cartesianInto(targets)(expected5)
    }
  }

  val RootKey: String = "rootKey"

  val RootProjection = Project(CPath.parse(".rootKey"))

  def rootWrapper(b: JsonElement): JsonElement = new BsonDocument(RootKey, b)

  def evalFocused(focused: List[ScalarStage.Focused], stream: JsonStream): JsonStream =
    interpret(ScalarStages(IdStatus.ExcludeId, RootProjection :: focused), stream, rootWrapper)

  def evalOneStage(stage: ScalarStage, stream: JsonStream): JsonStream =
    interpret(ScalarStages(IdStatus.ExcludeId, List(RootProjection, stage)), stream, rootWrapper)

  def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream =
    evalOneStage(wrap, stream)

  def evalProject(project: Project, stream: JsonStream): JsonStream =
    evalOneStage(project, stream)

  def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream =
    evalOneStage(pivot, stream)

  def evalMask(mask: Mask, stream: JsonStream): JsonStream =
    evalOneStage(mask, stream)

  def evalCartesian(cartesian: Cartesian, stream: JsonStream): JsonStream =
    evalOneStage(cartesian, stream)
}
