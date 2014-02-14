package org.kiji.express.flow.framework.serialization

import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.twitter.scalding.serialization.Externalizer
import com.twitter.chill.config.ScalaMapConfig

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * Constructors for wrapping objects with a java-serializable container using Kryo.
 */
@ApiAudience.Private
@ApiStability.Stable
object KijiKryoExternalizer {
  def apply[T](t: T): KijiKryoExternalizer[T] = {
    val externalizer = new KijiKryoExternalizer[T]
    externalizer.set(t)
    externalizer
  }
}

/**
 * Serializable container for wrapping objects using Kryo.
 *
 * To use this:
 * {{{
 *   val myNonSerializableThings = //...
 *
 *   // Wrap your data in a serializable container.
 *   val myNowSerializableThings = KijiKryoExternalizer(myNonSerializableThings)
 *
 *   // To reconstitute your original data:
 *   val myOriginalThings = myNowSerializableThings.get
 *
 *   // - or -
 *
 *   val myOriginalThings = myNowSerializableThings.getOption
 * }}}
 */
@ApiAudience.Private
@ApiStability.Stable
@Inheritance.Sealed
@DefaultSerializer(classOf[JavaSerializer])
class KijiKryoExternalizer[T] extends Externalizer[T] {
  protected override def kryo =
      new KijiKryoInstantiator(ScalaMapConfig(Map("scalding.kryo.setreferences" -> "true")))
}
