package org.kiji.express

import scala.collection.mutable

import cascading.tuple.Fields
import cascading.pipe.Each
import cascading.pipe.Pipe
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.NullSource
import com.twitter.scalding.SideEffectMapFunction
import com.twitter.scalding.Source
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter

import org.kiji.express.flow.KijiInputSuite
import org.kiji.express.flow.framework.serialization.KijiLocker
import org.kiji.express.flow.util.TestPipeConversions

/**
 * A job for testing scalding sources as input. Requires that a source and the resulting tuples it
 * should produce be provided by the user. This job does not write data produced by the input source
 * but instead validates that the tuples produced are correct within a map function.
 *
 * @param inputSource to test.
 * @param expectedTuples tuples that the input source should produce.
 * @param expectedFields that the input source should produce.
 * @param converter for validating that the specified fields arity matches the resulting tuple
 *     arity. This converter should be provided implicitly.
 * @param setter is ignored for this job since no tuples are written out.
 * @tparam A is the tuple type produced by the input source.
 */
class InputSourceValidationJob[A](
    @transient inputSource: Source,
    @transient expectedTuples: Set[A],
    expectedFields: Fields
)(implicit
    converter: TupleConverter[A],
    setter: TupleSetter[Unit]
) extends Job(Args(Nil)) with TestPipeConversions {
  converter.assertArityMatches(expectedFields)

  val _expected = KijiLocker(expectedTuples)

  def inputPipe: Pipe = inputSource.read
  def recordPipe = new Each(
      inputPipe,
      Fields.ALL,
      new SideEffectMapFunction(
          bf = { mutable.ArrayBuffer[A]() },
          fn = { (buffer: mutable.ArrayBuffer[A], tuple: A) => buffer.append(tuple) },
          ef = InputSourceValidationJob.assertOutput(_expected.get),
          fields = Fields.NONE,
          converter,
          setter
      )
  )
  NullSource.writeFrom(recordPipe)
}

object InputSourceValidationJob {
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

