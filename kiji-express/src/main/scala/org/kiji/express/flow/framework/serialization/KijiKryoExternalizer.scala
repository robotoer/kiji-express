package org.kiji.express.flow.framework.serialization

import com.twitter.chill.{KryoInstantiator, Externalizer}
import org.kiji.annotations.{ApiStability, ApiAudience}


/**
 * Created by sea_bass on 2/4/14.
 */

/**
 * Constructor function for a
 * [[org.kiji.express.flow.framework.serialization.KijiLocker]]
 */

object KijiKryoExternalizer {
  /* Tokens used to distinguish if we used Kryo or Java */
  private val KRYO = 0
  private val JAVA = 1

  def apply[T <: AnyRef](t: T): KijiKryoExternalizer[T] = {
    val x = new KijiKryoExternalizer[T]
    x.set(t)
    x
  }
}

class KijiKryoExternalizer[T <: AnyRef] extends Externalizer[T] {

  // configure kryo configuration inside of KijiKryoInsdtantiator
  override def kryo = new KijiKryoInstantiator()


}