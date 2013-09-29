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

trait NamedTuple {
  def field: Map[String, Any]
}

case class NamedTuple1[
    @specialized(Int, Long, Double) +T1
](
    entry1: (String, T1)
)
    extends Tuple1[
        T1
    ](
        entry1._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1)
}
        
case class NamedTuple2[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2
](
    entry1: (String, T1),
    entry2: (String, T2)
)
    extends Tuple2[
        T1,
        T2
    ](
        entry1._2,
        entry2._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2)
}
        
case class NamedTuple3[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3)
)
    extends Tuple3[
        T1,
        T2,
        T3
    ](
        entry1._2,
        entry2._2,
        entry3._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3)
}
        
case class NamedTuple4[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4)
)
    extends Tuple4[
        T1,
        T2,
        T3,
        T4
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4)
}
        
case class NamedTuple5[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5)
)
    extends Tuple5[
        T1,
        T2,
        T3,
        T4,
        T5
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5)
}
        
case class NamedTuple6[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6)
)
    extends Tuple6[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6)
}
        
case class NamedTuple7[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7)
)
    extends Tuple7[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7)
}
        
case class NamedTuple8[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8)
)
    extends Tuple8[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8)
}
        
case class NamedTuple9[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9)
)
    extends Tuple9[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9)
}
        
case class NamedTuple10[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10)
)
    extends Tuple10[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10)
}
        
case class NamedTuple11[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11)
)
    extends Tuple11[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11)
}
        
case class NamedTuple12[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12)
)
    extends Tuple12[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12)
}
        
case class NamedTuple13[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13)
)
    extends Tuple13[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13)
}
        
case class NamedTuple14[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14)
)
    extends Tuple14[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14)
}
        
case class NamedTuple15[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14,
    @specialized(Int, Long, Double) +T15
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14),
    entry15: (String, T15)
)
    extends Tuple15[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2,
        entry15._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14,
      entry15)
}
        
case class NamedTuple16[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14,
    @specialized(Int, Long, Double) +T15,
    @specialized(Int, Long, Double) +T16
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14),
    entry15: (String, T15),
    entry16: (String, T16)
)
    extends Tuple16[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2,
        entry15._2,
        entry16._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14,
      entry15,
      entry16)
}
        
case class NamedTuple17[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14,
    @specialized(Int, Long, Double) +T15,
    @specialized(Int, Long, Double) +T16,
    @specialized(Int, Long, Double) +T17
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14),
    entry15: (String, T15),
    entry16: (String, T16),
    entry17: (String, T17)
)
    extends Tuple17[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2,
        entry15._2,
        entry16._2,
        entry17._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14,
      entry15,
      entry16,
      entry17)
}
        
case class NamedTuple18[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14,
    @specialized(Int, Long, Double) +T15,
    @specialized(Int, Long, Double) +T16,
    @specialized(Int, Long, Double) +T17,
    @specialized(Int, Long, Double) +T18
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14),
    entry15: (String, T15),
    entry16: (String, T16),
    entry17: (String, T17),
    entry18: (String, T18)
)
    extends Tuple18[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2,
        entry15._2,
        entry16._2,
        entry17._2,
        entry18._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14,
      entry15,
      entry16,
      entry17,
      entry18)
}
        
case class NamedTuple19[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14,
    @specialized(Int, Long, Double) +T15,
    @specialized(Int, Long, Double) +T16,
    @specialized(Int, Long, Double) +T17,
    @specialized(Int, Long, Double) +T18,
    @specialized(Int, Long, Double) +T19
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14),
    entry15: (String, T15),
    entry16: (String, T16),
    entry17: (String, T17),
    entry18: (String, T18),
    entry19: (String, T19)
)
    extends Tuple19[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2,
        entry15._2,
        entry16._2,
        entry17._2,
        entry18._2,
        entry19._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14,
      entry15,
      entry16,
      entry17,
      entry18,
      entry19)
}
        
case class NamedTuple20[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14,
    @specialized(Int, Long, Double) +T15,
    @specialized(Int, Long, Double) +T16,
    @specialized(Int, Long, Double) +T17,
    @specialized(Int, Long, Double) +T18,
    @specialized(Int, Long, Double) +T19,
    @specialized(Int, Long, Double) +T20
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14),
    entry15: (String, T15),
    entry16: (String, T16),
    entry17: (String, T17),
    entry18: (String, T18),
    entry19: (String, T19),
    entry20: (String, T20)
)
    extends Tuple20[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2,
        entry15._2,
        entry16._2,
        entry17._2,
        entry18._2,
        entry19._2,
        entry20._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14,
      entry15,
      entry16,
      entry17,
      entry18,
      entry19,
      entry20)
}
        
case class NamedTuple21[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14,
    @specialized(Int, Long, Double) +T15,
    @specialized(Int, Long, Double) +T16,
    @specialized(Int, Long, Double) +T17,
    @specialized(Int, Long, Double) +T18,
    @specialized(Int, Long, Double) +T19,
    @specialized(Int, Long, Double) +T20,
    @specialized(Int, Long, Double) +T21
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14),
    entry15: (String, T15),
    entry16: (String, T16),
    entry17: (String, T17),
    entry18: (String, T18),
    entry19: (String, T19),
    entry20: (String, T20),
    entry21: (String, T21)
)
    extends Tuple21[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
        T21
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2,
        entry15._2,
        entry16._2,
        entry17._2,
        entry18._2,
        entry19._2,
        entry20._2,
        entry21._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14,
      entry15,
      entry16,
      entry17,
      entry18,
      entry19,
      entry20,
      entry21)
}
        
case class NamedTuple22[
    @specialized(Int, Long, Double) +T1,
    @specialized(Int, Long, Double) +T2,
    @specialized(Int, Long, Double) +T3,
    @specialized(Int, Long, Double) +T4,
    @specialized(Int, Long, Double) +T5,
    @specialized(Int, Long, Double) +T6,
    @specialized(Int, Long, Double) +T7,
    @specialized(Int, Long, Double) +T8,
    @specialized(Int, Long, Double) +T9,
    @specialized(Int, Long, Double) +T10,
    @specialized(Int, Long, Double) +T11,
    @specialized(Int, Long, Double) +T12,
    @specialized(Int, Long, Double) +T13,
    @specialized(Int, Long, Double) +T14,
    @specialized(Int, Long, Double) +T15,
    @specialized(Int, Long, Double) +T16,
    @specialized(Int, Long, Double) +T17,
    @specialized(Int, Long, Double) +T18,
    @specialized(Int, Long, Double) +T19,
    @specialized(Int, Long, Double) +T20,
    @specialized(Int, Long, Double) +T21,
    @specialized(Int, Long, Double) +T22
](
    entry1: (String, T1),
    entry2: (String, T2),
    entry3: (String, T3),
    entry4: (String, T4),
    entry5: (String, T5),
    entry6: (String, T6),
    entry7: (String, T7),
    entry8: (String, T8),
    entry9: (String, T9),
    entry10: (String, T10),
    entry11: (String, T11),
    entry12: (String, T12),
    entry13: (String, T13),
    entry14: (String, T14),
    entry15: (String, T15),
    entry16: (String, T16),
    entry17: (String, T17),
    entry18: (String, T18),
    entry19: (String, T19),
    entry20: (String, T20),
    entry21: (String, T21),
    entry22: (String, T22)
)
    extends Tuple22[
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
        T21,
        T22
    ](
        entry1._2,
        entry2._2,
        entry3._2,
        entry4._2,
        entry5._2,
        entry6._2,
        entry7._2,
        entry8._2,
        entry9._2,
        entry10._2,
        entry11._2,
        entry12._2,
        entry13._2,
        entry14._2,
        entry15._2,
        entry16._2,
        entry17._2,
        entry18._2,
        entry19._2,
        entry20._2,
        entry21._2,
        entry22._2)
    with NamedTuple {
  override val field: Map[String, Any] = Map(
      entry1,
      entry2,
      entry3,
      entry4,
      entry5,
      entry6,
      entry7,
      entry8,
      entry9,
      entry10,
      entry11,
      entry12,
      entry13,
      entry14,
      entry15,
      entry16,
      entry17,
      entry18,
      entry19,
      entry20,
      entry21,
      entry22)
}
