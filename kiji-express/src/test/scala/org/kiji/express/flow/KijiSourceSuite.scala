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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.flow

import scala.collection.mutable.Buffer

import cascading.tuple.Fields
import com.twitter.scalding._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.junit.JUnitRunner

import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.util.ResourceUtil.doAndRelease
import org.kiji.express.flow.util.TestPipeConversions
import org.kiji.express.flow.SchemaSpec.Specific
import org.kiji.express.KijiSuite
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType
import org.kiji.schema.Kiji
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

@RunWith(classOf[JUnitRunner])
class KijiSourceSuite extends KijiClientTest with KijiSuite with BeforeAndAfter {
  import KijiSourceSuite._


  setupKijiTest()
  val kiji: Kiji = createTestKiji()

  /** Simple table layout to use for tests. The row keys are hashed. */
  val simpleLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Table layout using Avro schemas to use for tests. The row keys are formatted. */
  val avroLayout: KijiTableLayout = layout("layout/avro-types.json")

  /* Undo all changes to hdfs mode. */
  before {
    Mode.mode = Local(true)
  }

  after {
    Mode.mode = Local(true)
  }

  private def writeToTable(table: KijiTable, values: List[String]) = {
    val writer: KijiTableWriter = table.openTableWriter()
    for (entry <- values) {
      val fields = entry.split("[\\s+:]")
      writer.put(table.getEntityId(fields(0)), fields(1), fields(2), fields(3))
      writer.flush()
    }
  }

  private def writeToTableWithTimestamp(table: KijiTable, values: List[String]) = {
    val writer: KijiTableWriter = table.openTableWriter()
    for (entry <- values) {
      val fields = entry.split("[\\s+:]")
      writer.put(table.getEntityId(fields(0)), fields(1), fields(2), fields(3).toLong, fields(4))
      writer.flush()
    }
  }

  /** Input values used for word count tests. */
  def wordCountInput: List[String] = List(
      "row01 family:column1 hello",
      "row02 family:column1 hello",
      "row03 family:column1 world",
      "row04 family:column1 hello")

  val wordCountExpected = Set(
    ("hello", 3),
    ("world", 1)
  )

  test("a word-count job that reads from a Kiji table is run using Scalding's local mode") {
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      writeToTable(table, wordCountInput)
      table.getURI().toString()
    }
    // Create test Kiji table.
    val source = KijiInput.builder
            .withTableURI(uri)
            .withColumns("family:column1" -> 'word)
            .build

    // Build test job.
    new WordCountJob(source, wordCountExpected).run
  }

  test("a word-count job that reads from a Kiji table is run using Hadoop") {
    Mode.mode = Hdfs(true, conf = new JobConf(getConf))

    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      writeToTable(table, wordCountInput)
      table.getURI().toString()
    }

    val source = KijiInput.builder
            .withTableURI(uri)
            .withColumns("family:column1" -> 'word)
            .build
    new WordCountJob(source, wordCountExpected).run
  }

  /** Input tuples to use for import tests. */
  val importMultipleTimestamps: List[String] = List(
      "1 eid1 word1",
      "3 eid1 word2",
      "5 eid2 word3",
      "7 eid2 word4")

  val multipleTimestampsExpected = Set(
    FlowCell("family","column1",3,"word2").toString(),
    FlowCell("family","column1",1,"word1").toString(),
    FlowCell("family","column1",7,"word4").toString(),
    FlowCell("family","column1",5,"word3").toString()
  )

  test("An import job with multiple timestamps imports all timestamps in local mode.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }
    val source = IterableSource(importMultipleTimestamps, new Fields("line"))
    // Build test job.
    new MultipleTimestampsImportJob(source, uri).run
    // Read from table to verify output.
    new ValidateOutputJob(uri, multipleTimestampsExpected).run

  }

  test("An import with multiple timestamps imports all timestamps using Hadoop.") {
    Mode.mode = Hdfs(true, conf = new JobConf(getConf))

    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }
    val source = IterableSource(importMultipleTimestamps, new Fields("line"))

    // Build test job.
    new MultipleTimestampsImportJob(source, uri).run
    // Read from table to verify output.
    new ValidateOutputJob(uri, multipleTimestampsExpected).run
  }

  /** Input values to use for version count tests. */
  val versionCountInput: List[String] = List(
      "row01 family:column1 10 two",
      "row01 family:column1 20 two",
      "row02 family:column1 10 three",
      "row02 family:column1 20 three",
      "row02 family:column1 30 three",
      "row03 family:column1 10 hello"
    )

  test("a job that requests wris gets them") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      writeToTableWithTimestamp(table, versionCountInput)
      table.getURI().toString()
    }

    // Expect up to two entries from each row.
    val expected: Set[(String, Int)] = Set(("two", 2), ("three", 2), ("hello", 1))

    // Build test job.
    val source =
        KijiInput.builder
            .withTableURI(uri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
                .withColumn("family", "column1")
                .withMaxVersions(2)
                .build -> 'words)
            .build
    new VersionsJob(source, expected).run
  }

  test("a job that requests a time range gets them") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      writeToTableWithTimestamp(table, versionCountInput)
      table.getURI().toString()
    }

    // Expect only the values with timestamp between 15 and 25.
    val expected: Set[(String, Int)] = Set(("two", 1), ("three", 1))

    // Build test job.
    val source = KijiInput.builder
        .withTableURI(uri)
        .withTimeRangeSpec(TimeRangeSpec.Between(15L, 25L))
        .withColumns("family:column1" -> 'words)
        .build
    new VersionsJob(source, expected).run
  }

  // TODO(EXP-7): Write this test.
  test("a word-count job that uses the type-safe api is run") {
    pending
  }

  // TODO(EXP-6): Write this test.
  test("a job that uses the matrix api is run") {
    pending
  }

  test("test conversion of column value of type string between java and scala in Hadoop mode") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      writeToTableWithTimestamp(table, versionCountInput)
      table.getURI().toString()
    }

    val expected: Set[(Boolean, Int)] = Set((true, 6))
    val testSource = KijiInput.builder
        .withTableURI(uri)
        .withColumnSpecs(QualifiedColumnInputSpec.builder
            .withColumn("family", "column1")
            .withMaxVersions(all)
            .build -> 'word)
        .build
    new AvroToScalaChecker(testSource, expected).run
  }


  val genericApiExpected = Set((13, 1))

  test("A job that reads using the generic API is run in local mode.") {
    val genericRecord = new HashSpec()
    genericRecord.setHashType(HashType.MD5)
    genericRecord.setHashSize(13)
    genericRecord.setSuppressKeyMaterialization(true)

    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      // Write the specific record to the table.
      val writer: KijiTableWriter = table.openTableWriter()
      writer.put(table.getEntityId("row01"), "family", "column3", 10L, genericRecord)
      writer.flush()

      table.getURI().toString()
    }
    val source = KijiInput.builder
          .withTableURI(uri)
          .withColumns("family:column3" -> 'records)
          .build

    new GenericAvroReadJob(source, genericApiExpected).run
  }

  test("A job that reads using the generic API is run in hdfs mode.") {
    Mode.mode = Hdfs(true, conf = new JobConf(getConf))

    val genericRecord = new HashSpec()
    genericRecord.setHashType(HashType.MD5)
    genericRecord.setHashSize(13)
    genericRecord.setSuppressKeyMaterialization(true)

    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      // Write the record to the table.
      val writer: KijiTableWriter = table.openTableWriter()
      writer.put(table.getEntityId("row01"), "family", "column3", 10L, genericRecord)
      writer.flush()

      table.getURI().toString()
    }

    val source = KijiInput.builder
      .withTableURI(uri)
      .withColumns("family:column3" -> 'records)
      .build

    new GenericAvroReadJob(source, genericApiExpected).run
  }

  test("A job that reads using the specific API is run in local mode.") {
    val specificRecord = new HashSpec()
    specificRecord.setHashType(HashType.MD5)
    specificRecord.setHashSize(13)
    specificRecord.setSuppressKeyMaterialization(true)

    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      // Write the record to the table.
      val writer: KijiTableWriter = table.openTableWriter()
      writer.put(table.getEntityId("row01"), "family", "column3", 10L, specificRecord)
      writer.flush()

      table.getURI().toString()
    }

    val source = new KijiSource(
        tableAddress = uri,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        inputColumns = Map('records -> ColumnInputSpec(
          "family:column3", schemaSpec = Specific(classOf[HashSpec]))),
        outputColumns = Map('records -> QualifiedColumnOutputSpec.builder
            .withColumn("family", "column3")
            .build)
    )

    new SpecificAvroReadJob(source, genericApiExpected).run
  }


  // TODO: Tests below this line still use JobTest and should be rewritten.
  test("A job that writes using the generic API is run.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input to use with Text source.
    val genericWriteInput: List[(String, String)] = List(
      ( "0", "zero" ),
      ( "1", "one" ))

    // Validates the output buffer contains the same as the input buffer.
    def validateGenericWrite(outputBuffer: Buffer[(EntityId, Seq[FlowCell[GenericRecord]])]) {
      val inputMap: Map[Long, String] = genericWriteInput.map { t => t._1.toLong -> t._2 }.toMap
      outputBuffer.foreach { t: (EntityId, Seq[FlowCell[GenericRecord]]) =>
        val entityId = t._1
        val record = t._2.head.datum

        val s = record.get("s").asInstanceOf[String]
        val l = record.get("l").asInstanceOf[Long]

        assert(entityId(0) === s)
        assert(inputMap(l) === s)
      }
    }

    val jobTest = JobTest(new GenericAvroWriteJob(_))
      .arg("input", "inputFile")
      .arg("output", uri)
      .source(TextLine("inputFile"), genericWriteInput)
      .sink(KijiOutput.builder
      .withTableURI(uri)
      .withColumns('record -> "family:column4")
      .build
      )(validateGenericWrite)

    // Run in local mode.
    jobTest.run.finish

    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }


  test ("A job that writes to map-type column families is run.") {
    // URI of the Kiji table to use.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input text.
    val mapTypeInput: List[(String, String)] = List(
        ("0", "dogs 4"),
        ("1", "cats 5"),
        ("2", "fish 3"))

    // Validate output.
    def validateMapWrite(
        outputBuffer: Buffer[(EntityId,Seq[FlowCell[GenericRecord]])]
    ): Unit = {
      assert (1 === outputBuffer.size)
      val outputSlice = outputBuffer(0)._2
      val outputSliceMap = outputSlice.groupBy(_.qualifier)
      assert (4 === outputSliceMap("dogs").head.datum)
      assert (5 === outputSliceMap("cats").head.datum)
      assert (3 === outputSliceMap("fish").head.datum)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new MapWriteJob(_))
        .arg("input", "inputFile")
        .arg("table", uri)
        .source(TextLine("inputFile"), mapTypeInput)

        .sink(KijiOutput.builder
            .withTableURI(uri)
            .withColumnSpecs('resultCount -> ColumnFamilyOutputSpec.builder
                .withFamily("searches")
                .withQualifierSelector('terms)
                .build)
            .build
        )(validateMapWrite)

    // Run the test.
    jobTest.run.finish
    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test ("A job that writes to map-type column families with numeric column qualifiers is run.") {
    // URI of the Kiji table to use.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Create input using mapSlice.
    val mapTypeInput: List[(EntityId, Seq[FlowCell[String]])] = List(
        ( EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs")) ),
        ( EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat")) ),
        ( EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish")) ))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 3)
      val outputSet = outputBuffer.map { value: Tuple1[String] =>
        value._1
      }.toSet
      assert (outputSet.contains("0 dogs"), "Failed on \"0 dogs\" test")
      assert (outputSet.contains("1 cat"), "Failed on \"1 cat\" test")
      assert (outputSet.contains("2 fish"), "Failed on \"2 fish\" test")
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new MapSliceJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumns("animals" -> 'terms)
            .build, mapTypeInput)
        .sink(Tsv("outputFile"))(validateTest)

    // Run the test.
    jobTest.run.finish
    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A job that joins two pipes, on string keys, is run in both local and hadoop mode.") {
    // URI of the Kiji table to use.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Create input from Kiji table.
    val joinKijiInput: List[(EntityId, Seq[FlowCell[String]])] = List(
        ( EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs")) ),
        ( EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat")) ),
        ( EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish")) ))

    // Create input from side data.
    val sideInput: List[(String, String)] = List( ("0", "0row"), ("1", "2row") )

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinOnStringsJob(_))
        .arg("input", uri)
        .arg("side-input", "sideInputFile")
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumns("animals" -> 'animals)
            .build, joinKijiInput)
        .source(TextLine("sideInputFile"), sideInput)
        .sink(Tsv("outputFile"))(validateTest)

    // Run the test in local mode.
    jobTest.run.finish

    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }
}

/** Companion object for KijiSourceSuite. Contains test jobs. */
object KijiSourceSuite {

  /**
   * A job that creates a new KijiInput to read the values from a table and compare
   * the output to an expected value. This is used to verify the results from
   * earlier KijiOutput table writes.
   *
   * @param uri The table URI to read from.
   * @param expected result to verify against.
   */
  class ValidateOutputJob(uri: String, expected: Set[String]) extends KijiJob()
      with TestPipeConversions {
    KijiInput.builder
      .withTableURI(uri)
      .withColumnSpecs(QualifiedColumnInputSpec.builder
            .withColumn("family", "column1")
            .withMaxVersions(2)
            .build -> 'cleanword)
      .build
      .read
      .flatMap('cleanword -> 'out) { words:Seq[FlowCell[CharSequence]] => words }
      .assertOutputValues(
        ('out),
        expected
      )
  }

  /**
   * A job that extracts the most recent string value from the column "family:column1" for all rows
   * in a Kiji table, and then counts the number of occurrences of those strings across rows.
   *
   * The output is verified to match an expected value.
   *
   * @param source The KijiSource that the job should be run on.
   * @param expected result to verify against.
   */
  class WordCountJob(source: KijiSource, expected: Set[(String, Int)]) extends KijiJob()
      with TestPipeConversions {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    val map = source
        // Sanitize the word.
        .map('word -> 'cleanword) { words:Seq[FlowCell[CharSequence]] =>
          words.head.datum
              .toString()
              .toLowerCase()
        }
        // Count the occurrences of each word.
        .groupBy('cleanword) { occurences => occurences.size }
        // Assert that we got the correct output.
        .assertOutputValues(
          ('cleanword, 'size),
          expected
        )
  }

  /**
   * A job that takes the most recent string value from the column "family:column1" and adds
   * the letter 's' to the end of it. It passes through the column "family:column2" without
   * any changes.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class TwoColumnJob(args: Args) extends KijiJob(args) {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("family:column1" -> 'word1, "family:column2" -> 'word2)
        .build
        .map('word1 -> 'pluralword) { words: Seq[FlowCell[CharSequence]] =>
          words.head.datum.toString() + "s"
        }
        .write(Tsv(args("output")))
  }

  /**
   * A job that requests specific number of versions and buckets the results by the number of
   * versions.  The result is each word grouped with the number of versions it has.
   *
   * The output is verified to match an expected value.
   *
   * @param source that the job will use.
   * @param expected result to verify against.
   */
  class VersionsJob(source: KijiSource, expected: Set[(String, Int)]) extends KijiJob()
      with TestPipeConversions {
    source
        // Count the size of words (number of versions).
        .map('words -> ('word, 'versioncount)) { words: Seq[FlowCell[CharSequence]]=>
          (words.head.datum.toString(), words.size)
        }
        .discard('words, 'entityId)
      .assertOutputValues(
        ('word, 'versioncount),
        expected
      )
  }

  /**
   * A job that, given lines of text, writes each line to row for the line in a Kiji table,
   * at the column "family:column1", using multiple timestamps and values per row.
   *
   * @param source An IterableSource containing input values as Strings.
   * @param uri of the table to write output to
   *
   */
  class MultipleTimestampsImportJob(source: IterableSource[String], uri: String)
      extends KijiJob() {
    source
        .read
        // New  the words in each line.
        .map('line -> ('timestamp, 'entityId, 'word)) { line: String =>
          val Array(timestamp, eid, token) = line.split("\\s+")
          (timestamp.toLong, EntityId(eid), token)
        }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput.builder
            .withTableURI(uri)
            .withTimestampField('timestamp)
            .withColumns('word -> "family:column1")
            .build)
  }

  /**
   * A job that given input from a Kiji table, ensures the type is accurate.
   *
   * @param source that the job will use.
   * @param expected result to verify against
   */
  class AvroToScalaChecker(source: KijiSource, expected: Set[(Boolean, Int)]) extends KijiJob()
      with TestPipeConversions {
    source
        .flatMap('word -> 'matches) { word: Seq[FlowCell[CharSequence]] =>
          word.map { cell: FlowCell[CharSequence] =>
            val value = cell.datum
            if (value.isInstanceOf[CharSequence]) {
              "true"
            } else {
              "false"
            }
          }
        }
        .groupBy('matches) (_.size)
        .assertOutputValues(('matches, 'size), expected)
  }

  /**
   * A job that uses the generic API, getting the "hash_size" field from a generic record, and
   * writes the number of records that have a certain hash_size.
   *
   */
  class GenericAvroReadJob(source: KijiSource, expected: Set[(Int, Int)]) extends KijiJob()
      with TestPipeConversions {
      source
        .map('records -> 'hashSizeField) { slice: Seq[FlowCell[GenericRecord]] =>
          slice.head match {
            case FlowCell(_, _, _, record: GenericRecord) => {
              record
                  .get("hash_size")
                  .asInstanceOf[Int]
            }
          }
        }
        .groupBy('hashSizeField)(_.size)
        .assertOutputValues(('hashSizeField, 'size), expected)
  }

  /**
   * A job that uses the specific API, getting the "hash_size" field from a specific record, and
   * writes the number of records that have a certain hash_size.
   *
   * @param source that the job will use.
   * @param expected Result to verify against.
   */
  class SpecificAvroReadJob(source: KijiSource, expected: Set[(Int, Int)]) extends KijiJob()
      with TestPipeConversions {
    source
        .map('records -> 'hashSizeField) { slice: Seq[FlowCell[HashSpec]] =>
          val FlowCell(_, _, _, record) = slice.head
          record.getHashSize
        }
        .groupBy('hashSizeField)(_.size)
        .assertOutputValues(('hashSizeField, 'size), expected)
  }

  /**
   * A job that uses the generic API, creating a record containing the text from the input,
   * and writing it to a Kiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class GenericAvroWriteJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("output")
    TextLine(args("input"))
      .read
      .map('offset -> 'timestamp) { offset: String => offset.toLong }
      .map('offset -> 'l) { offset: String => offset.toLong }
      // Generate an entityId for each line.
      .map('line -> 'entityId) { EntityId(_: String) }
      .rename('line -> 's)
      .packGenericRecord(('l, 's) -> 'record)(SimpleRecord.getClassSchema)
      // Write the results to the "family:column4" column of a Kiji table.
      .project('entityId, 'record)
      .write(KijiOutput.builder
      .withTableURI(args("output"))
      .withColumns('record -> "family:column4")
      .build)
  }


  /**
   * A job that writes to a map-type column family.  It takes text from the input and uses it as
   * search terms and the number of results returned for that term. All of them belong to the same
   * entity, "my_eid".
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class MapWriteJob(args: Args) extends KijiJob(args) {
    TextLine(args("input"))
        .read
        // Create an entity ID for each line (always the same one, here)
        .map('line -> 'entityId) { line: String => EntityId("my_eid") }
        // new  the number of result for each search term
        .map('line -> ('terms, 'resultCount)) { line: String =>
          (line.split(" ")(0), line.split(" ")(1).toInt)
        }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput.builder
            .withTableURI(args("table"))
            .withColumnSpecs('resultCount -> ColumnFamilyOutputSpec.builder
                .withFamily("searches")
                .withQualifierSelector('terms)
                .build)
            .build)
  }

  /**
   * A job that tests map-type column families using sequences of cells and outputs the results to
   * a TSV.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class MapSliceJob(args: Args) extends KijiJob(args) {
    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("animals" -> 'terms)
        .build
        .map('terms -> 'values) { terms: Seq[FlowCell[CharSequence]] => terms.head.datum }
        .project('values)
        .write(Tsv(args("output")))
  }

  /**
   * A job that tests joining two pipes, on String keys.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class JoinOnStringsJob(args: Args) extends KijiJob(args) {
    val sidePipe = TextLine(args("side-input"))
        .read
        .map('line -> 'entityId) { line: String => EntityId(line) }

    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("animals" -> 'animals)
        .build
        .map('animals -> 'terms) { animals: Seq[FlowCell[CharSequence]] =>
          animals.head.datum.toString.split(" ")(0) + "row" }
        .discard('entityId)
        .joinWithSmaller('terms -> 'line, sidePipe)
        .write(Tsv(args("output")))
  }
}
