package org.kiji.express.flow

import java.util.UUID

import scala.collection.mutable

import cascading.pipe.Each
import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.NullSource
import com.twitter.scalding.SideEffectMapFunction
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.framework.serialization.KijiLocker
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.express.flow.util.TestPipeConversions
import org.kiji.schema.Kiji
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiURI
import org.kiji.schema.util.InstanceBuilder
import org.apache.avro.util.Utf8

class TestKijiJob[A](
    @transient inputSource: KijiSource,
    @transient expected: Set[A]
//    validationFn: mutable.Seq[A] => Unit
)(implicit
    converter: TupleConverter[A],
    setter: TupleSetter[Unit]
    // TODO: See if this can be moved to a normal parameter with an implicit default value.
//    currentMode: Mode
) extends KijiJob
    with TestPipeConversions {
  val _expected = KijiLocker(expected)

//  def inputPipe: Pipe = inputSource.read(this.flowDef, currentMode)
  def inputPipe: Pipe = inputSource.read
  def recordPipe = new Each(
      inputPipe,
      Fields.ALL,
      new SideEffectMapFunction(
          bf = { mutable.ArrayBuffer[A]() },
          fn = { (buffer: mutable.ArrayBuffer[A], tuple: A) => buffer.append(tuple) },
//          ef = validationFn,
          ef = { buffer: mutable.ArrayBuffer[A] => KijiInputSuite.assertOutput(_expected.get)(buffer) },
          fields = Fields.NONE,
          converter,
          setter
      )
  )
  NullSource.writeFrom(recordPipe)
}

// Things that are currently tested in this file:
// - test("a word-count job that reads from a Kiji table is run using Scalding's local mode")
// - test("a word-count job that reads from a Kiji table is run using Hadoop")
// - test("An import job with multiple timestamps imports all timestamps in local mode.")
// - test("An import with multiple timestamps imports all timestamps using Hadoop.")
// - test("a job that requests writes gets them")
// - test("a job that requests a time range gets them")
// - test("a word-count job that uses the type-safe api is run")
// - test("a job that uses the matrix api is run")
// - test("test conversion of column value of type string between java and scala in Hadoop mode")
// - test("A job that reads using the generic API is run in local mode.")
// - test("A job that reads using the generic API is run in hdfs mode.")
// - test("A job that reads using the specific API is run in local mode.")
// - test("A job that writes using the generic API is run.")
// - test("A job that joins two pipes, on string keys, is run in both local and hadoop mode.")

// Dimensions(all independent of each other) that should be tested in this file:
// - table uri [valid, invalid] x [exists, doesn't exist]
// - timerange spec [All, (At, t), (Before, t), (Between, t1, t2), (Until, t)]
// - rowrange spec [All, (At, t), (Before, t), (Between, t1, t2), (Until, t)]
// - rowfilter spec [Random, ...]
// - column specs ([num of columns], [map-family, qualified-column])
//   - map-family:
//     - family name [strings]
//     - max versions [1, negative, positive > 1]
//     - paging spec
//     - schema spec
//     - filter spec
//   - qualified-column:
//     - column name [(string, string)]
//     - max versions [1, negative, positive > 1]
//     - paging spec
//     - schema spec
//     - filter spec

@RunWith(classOf[JUnitRunner])
class KijiInputSuite
    extends KijiClientTest
    with KijiSuite
    with BeforeAndAfter {
  // Must be provided with:
  // Source to run job with & populated environment.
  // Validation code?
  def kijiInputTest[T](
      testName: String,
      sourceConstructor: KijiURI => KijiSource,
      expectedFields: Fields,
      expectedValues: Set[T],
      testHdfsMode: Boolean = true,
      testLocalMode: Boolean = true
  )(implicit
      conv: TupleConverter[T],
      set: TupleSetter[Unit]
  ) {
    if (testHdfsMode) {
      test("[HDFS] " + testName) {
        val testMode = Hdfs(strict = true, conf = new JobConf(getConf))
        // Set the global mode variable in case other methods rely on it being injected via an
        // implicit evidence parameter.
        Mode.mode = testMode
        val uri = KijiInputSuite.setupTestTable(getKiji)

        val source = sourceConstructor(uri)
//        new TestKijiJob[T](source, KijiInputSuite.assertOutput(expectedValues))(conv, set).run
        new TestKijiJob[T](source, expectedValues)(conv, set).run
      }
    }

    if (testLocalMode) {
      test("[Local] " + testName) {
        val testMode = Local(strict = true)
        // Set the global mode variable in case other methods rely on it being injected via an
        // implicit evidence parameter.
        Mode.mode = testMode
        val uri = KijiInputSuite.setupTestTable(getKiji)

        val source = sourceConstructor(uri)
//        new TestKijiJob[T](source, KijiInputSuite.assertOutput(expectedValues))(conv, set).run
        new TestKijiJob[T](source, expectedValues)(conv, set).run
      }
    }
  }

  setupKijiTest()

  /* Undo all changes to hdfs mode. */
  before {
    Mode.mode = Local(strict = true)
  }

  after {
    Mode.mode = Local(strict = true)
  }

  // Dimensions(all independent of each other) that should be tested in this file:
  // - table uri [valid, invalid] x [exists, doesn't exist]
  // - timerange spec [All, (At, t), (Before, t), (Between, t1, t2), (Until, t)]
  // - rowrange spec [All, (At, t), (Before, t), (Between, t1, t2), (Until, t)]
  // - rowfilter spec [Random, ...]
  // - column specs ([num of columns], [map-family, qualified-column])
  //   - map-family:
  //     - family name [strings]
  //     - max versions [1, negative, positive > 1]
  //     - paging spec
  //     - schema spec
  //     - filter spec
  //   - qualified-column:
  //     - column name [(string, string)]
  //     - max versions [1, negative, positive > 1]
  //     - paging spec
  //     - schema spec
  //     - filter spec
  kijiInputTest(
      testName = "KijiSource with a valid table uri",
      sourceConstructor = { tableUri: KijiURI =>
        KijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumns("info:strings" -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId("row1"), slice("info:strings", 2L -> new Utf8("string2"))),
          (EntityId("row2"), slice("info:strings", 4L -> new Utf8("string4")))
      )
  )
//  kijiInputTest(
//      testName = "KijiSource with an invalid table uri",
//      sourceConstructor = { tableUri: KijiURI =>
//        KijiInput.builder
//            .withTableURI(tableUri.toString)
//            .withColumns("info:strings" -> 'strings)
//            .build
//      },
//      expectedFields = new Fields("entityId", "strings"),
//      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
//          (EntityId("row1"), slice("info:strings", 2L -> new Utf8("string2"))),
//          (EntityId("row2"), slice("info:strings", 4L -> new Utf8("string4")))
//      )
//  )

  kijiInputTest(
      testName = "KijiSource can filter cells with the 'All' time range",
      sourceConstructor = { tableUri: KijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        KijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.All)
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (
              EntityId("row1"),
              slice("info:strings", 2L -> new Utf8("string2"), 1L -> new Utf8("string1"))
          ),
          (
              EntityId("row2"),
              slice("info:strings", 4L -> new Utf8("string4"), 3L -> new Utf8("string3"))
          )
      )
  )
  kijiInputTest(
      testName = "KijiSource can filter cells with the 'At' time range",
      sourceConstructor = { tableUri: KijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        KijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.At(2L))
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId("row1"), slice("info:strings", 2L -> new Utf8("string2"))),
          (EntityId("row2"), slice("info:strings"))
      )
  )
  kijiInputTest(
      testName = "KijiSource can filter cells with the 'Before' time range",
      sourceConstructor = { tableUri: KijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        KijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.Before(3L))
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (
              EntityId("row1"),
              slice("info:strings", 2L -> new Utf8("string2"), 1L -> new Utf8("string1"))
          ),
          (
              EntityId("row2"),
              slice("info:strings")
          )
      )
  )
  kijiInputTest(
      testName = "KijiSource can filter cells with the 'Between' time range",
      sourceConstructor = { tableUri: KijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        KijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.Between(2L, 4L))
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (EntityId("row1"), slice("info:strings", 2L -> new Utf8("string2"))),
          (EntityId("row2"), slice("info:strings", 3L -> new Utf8("string3")))
      )
  )
  kijiInputTest(
      testName = "KijiSource can filter cells with the 'From' time range",
      sourceConstructor = { tableUri: KijiURI =>
        val stringsColumn = QualifiedColumnInputSpec.builder
            .withColumn("info", "strings")
            .withMaxVersions(Int.MaxValue)
            .build

        KijiInput.builder
            .withTableURI(tableUri.toString)
            .withTimeRangeSpec(TimeRangeSpec.From(2L))
            .withColumnSpecs(stringsColumn -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[Utf8]])](
          (
              EntityId("row1"),
              slice("info:strings", 2L -> new Utf8("string2"))
          ),
          (
              EntityId("row2"),
              slice("info:strings", 4L -> new Utf8("string4"), 3L -> new Utf8("string3"))
          )
      )
  )
}

object KijiInputSuite {
  def setupTestTable(kiji: Kiji): KijiURI = {
    val testTableName = "%s_%s"
        .format(
            this.getClass.getSimpleName,
            UUID.randomUUID().toString
        )
        .replace("-", "_")
        .replace("$", "_")
    val testTableDDL =
        """
          |CREATE TABLE %s
          |    ROW KEY FORMAT
          |    (
          |        row_name STRING,
          |        HASH (THROUGH row_name, SIZE = 2)
          |    )
          |    WITH LOCALITY GROUP default
          |    (
          |        MAXVERSIONS = INFINITY,
          |        TTL = FOREVER,
          |        COMPRESSED WITH NONE,
          |        FAMILY info
          |        (
          |            strings "string",
          |            numbers "int",
          |            records WITH SCHEMA CLASS org.kiji.express.avro.SimpleRecord
          |        ),
          |        MAP TYPE FAMILY mapfamily WITH SCHEMA CLASS org.kiji.express.avro.SimpleRecord
          |    );
        """.stripMargin.format(testTableName)

    // Create table.
    ResourceUtil.executeDDLString(kiji, testTableDDL)

    // Populate table.
    ResourceUtil.withKijiTable(kiji, testTableName) { table =>
      val testRecord1 = SimpleRecord.newBuilder().setL(1L).setS("1").build()
      val testRecord2 = SimpleRecord.newBuilder().setL(2L).setS("2").build()
      val testRecord3 = SimpleRecord.newBuilder().setL(3L).setS("3").build()
      val testRecord4 = SimpleRecord.newBuilder().setL(4L).setS("4").build()

      new InstanceBuilder(kiji)
          .withTable(table)
              .withRow("row1")
                  .withFamily("info")
                      .withQualifier("strings")
                          .withValue(1L, "string1")
                          .withValue(2L, "string2")
                      .withQualifier("numbers")
                          .withValue(1L, 1)
                          .withValue(2L, 2)
                      .withQualifier("records")
                          .withValue(1L, testRecord1)
                          .withValue(2L, testRecord2)
                  .withFamily("mapfamily")
                      .withQualifier("qual1")
                          .withValue(1L, testRecord1)
                          .withValue(2L, testRecord2)
                      .withQualifier("qual2")
                          .withValue(3L, testRecord3)
                          .withValue(4L, testRecord4)
              .withRow("row2")
                  .withFamily("info")
                      .withQualifier("strings")
                          .withValue(3L, "string3")
                          .withValue(4L, "string4")
                      .withQualifier("numbers")
                          .withValue(3L, 3)
                          .withValue(4L, 4)
                      .withQualifier("records")
                          .withValue(3L, testRecord3)
                          .withValue(4L, testRecord4)
                  .withFamily("mapfamily")
                      .withQualifier("qual1")
                          .withValue(1L, testRecord1)
                          .withValue(2L, testRecord2)
                      .withQualifier("qual2")
                          .withValue(3L, testRecord3)
                          .withValue(4L, testRecord4)
          .build()
    }

    KijiURI
        .newBuilder(kiji.getURI)
        .withTableName(testTableName)
        .build()
  }

  def assertOutput[T](expected: Set[T])(actual: Seq[T]) {
    val actualSet = actual.toSet
    assert(
        actualSet == expected,
        "actual: %s\nexpected: %s\noutput missing: %s\nunexpected: %s".format(
            actualSet,
            expected,
            expected -- actualSet,
            actualSet -- expected
        )
    )
  }
}
