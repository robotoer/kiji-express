/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.modeling.framework

object NamedTupleGenerator {
  case class NamedTupleDef(n: Int) {
    override def toString: String = {
      val namedTupleTypeParams = (1 to n)
          .map { NamedTupleTypeParam }
          .map { "    " + _.toString }
          .reduce { _ + ",\n" + _ }
      val namedTupleTypeParamsUsed = (1 to n)
          .map { NamedTupleTypeParamUsed }
          .map { "        " + _.toString }
          .reduce { _ + ",\n" + _ }
      val namedTupleValueParams = (1 to n)
          .map { NamedTupleValueParam }
          .map { "    " + _.toString }
          .reduce { _ + ",\n" + _ }
      val namedTupleValueParamsUsed = (1 to n)
          .map { NamedTupleValueParamUsed }
          .map { "        " + _.toString }
          .reduce { _ + ",\n" + _ }
      val namedTupleFieldMappings = (1 to n)
          .map { NamedTupleFieldMapping }
          .map { "      " + _.toString }
          .reduce { _ + ",\n" + _ }
      val fmtStr =
        """
          |case class NamedTuple%d[
          |%s
          |](
          |%s
          |)
          |    extends Tuple%d[
          |%s
          |    ](
          |%s)
          |    with NamedTuple {
          |  override val field: Map[String, Any] = Map(
          |%s)
          |}
        """

      fmtStr
          .stripMargin
          .format(
              n,
              namedTupleTypeParams,
              namedTupleValueParams,
              n,
              namedTupleTypeParamsUsed,
              namedTupleValueParamsUsed,
              namedTupleFieldMappings)
    }
  }

  case class NamedTupleTypeParam(n: Int) {
    override def toString: String = "@specialized(Int, Long, Double) +T%d".format(n)
  }

  case class NamedTupleTypeParamUsed(n: Int) {
    override def toString: String = "T%d".format(n)
  }

  case class NamedTupleValueParam(n: Int) {
    override def toString: String = "entry%d: (String, T%d)".format(n, n)
  }

  case class NamedTupleValueParamUsed(n: Int) {
    override def toString: String = "entry%d._2".format(n)
  }

  case class NamedTupleFieldMapping(n: Int) {
    override def toString: String = "entry%d".format(n)
  }

  def main(args: Array[String]) {
    val generated: String = (1 to 22)
        .map { NamedTupleDef }
        .map { _.toString }
        .reduce { _ + _ }

    println(generated)
  }
}
