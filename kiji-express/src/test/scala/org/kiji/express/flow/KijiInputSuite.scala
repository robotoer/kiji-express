package org.kiji.express.flow

import java.util.UUID

import cascading.tuple.Fields
import cascading.flow.FlowDef
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.express.flow.util.TestPipeConversions
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiURI
import org.kiji.schema.util.InstanceBuilder

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
  setupKijiTest()
  def setupTestTable(): KijiURI = {
    val testTableName = "%s_%s".format(
        this.getClass.getSimpleName,
        UUID.randomUUID().toString.replace("-", "_")
    )
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
    executeDDLString(getKiji, testTableDDL)

    // Populate table.
    ResourceUtil.withKijiTable(getKiji, testTableName) { table =>
      val testRecord1 = SimpleRecord.newBuilder().setL(1L).setS("1").build()
      val testRecord2 = SimpleRecord.newBuilder().setL(2L).setS("2").build()
      val testRecord3 = SimpleRecord.newBuilder().setL(3L).setS("3").build()
      val testRecord4 = SimpleRecord.newBuilder().setL(4L).setS("4").build()

      new InstanceBuilder(getKiji)
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
        .newBuilder(getKiji.getURI)
        .withTableName(testTableName)
        .build()
  }

  // Must be provided with:
  // Source to run job with & populated environment.
  // Validation code?
  def kijiInputTest[T](
      testName: String,
      sourceConstructor: KijiURI => KijiSource,
      expectedFields: Fields,
      expectedValues: Set[T],
      testLocalMode: Boolean = true
  )(implicit
      conv: TupleConverter[T],
      set: TupleSetter[Unit],
      mode: Mode
  ) {
    class TestKijiJob(inputSource: KijiSource)
        extends KijiJob
        with TestPipeConversions {
      inputSource
          .read(this.flowDef, mode)
          .assertOutputValues[T](expectedFields, expectedValues)(conv, set, this.flowDef, mode)
    }

    test("[HDFS] " + testName) {
      Mode.mode = Hdfs(strict = true, conf = new JobConf(getConf))
      val uri = setupTestTable()

      val source = sourceConstructor(uri)
      new TestKijiJob(source).run
    }

    if (testLocalMode) {
      test("[Local] " + testName) {
        Mode.mode = Local(strict = true)
        val uri = setupTestTable()

        val source = sourceConstructor(uri)
        new TestKijiJob(source).run
      }
    }
  }

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
  kijiInputTest[(EntityId, Seq[FlowCell[String]])](
      testName = "KijiSource with a valid table uri",
      sourceConstructor = { tableUri: KijiURI =>
        KijiInput.builder
            .withTableURI(tableUri.toString)
            .withColumns("info:strings" -> 'strings)
            .build
      },
      expectedFields = new Fields("entityId", "strings"),
      expectedValues = Set[(EntityId, Seq[FlowCell[String]])](
          (EntityId("row1"), slice("info:strings", 2L -> "string2")),
          (EntityId("row2"), slice("info:strings", 4L -> "string4"))
      )
  )
//  expectedValues = Set[(EntityId, Seq[FlowCell[String]])](
//    (EntityId("row1"), slice("info:strings", 1L -> "string1", 2L -> "string2")),
//    (EntityId("row2"), slice("info:strings", 3L -> "string3", 4L -> "string4"))
//  )

}
