package org.kiji.express.flow.framework.serialization

import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.twitter.scalding.serialization.Externalizer
import com.twitter.chill.config.ScalaMapConfig

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

@ApiAudience.Private
@ApiStability.Stable
object KijiKryoExternalizer {
  def apply[T](t: T): KijiKryoExternalizer[T] = {
    val externalizer = new KijiKryoExternalizer[T]
    externalizer.set(t)
    externalizer
  }
}

@DefaultSerializer(classOf[JavaSerializer])
class KijiKryoExternalizer[T] extends Externalizer[T] {
  protected override def kryo =
      new KijiKryoInstantiator(ScalaMapConfig(Map("scalding.kryo.setreferences" -> "true")))
}