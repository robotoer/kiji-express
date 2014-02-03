package org.kiji.express.flow.framework.serialization

import _root_.java.io.Serializable
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar
import com.twitter.chill.KryoInstantiator
import com.twitter.chill.ScalaCollectionsRegistrar
import com.twitter.chill.ScalaKryoInstantiator
import com.twitter.chill._
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.generic.{GenericContainer, GenericRecord}
import _root_.java.io.Serializable
import scala.reflect.Class


/**
 * Created by sea_bass on 2/4/14.
 */
object KijiKryoInstantiator extends ScalaKryoInstantiator{
  private val mutex = new AnyRef with Serializable // some serializable object
  @transient private var kpool: KryoPool = null

  /** Return a KryoPool that uses the ScalaKryoInstantiator
    */

  def defaultPool: KryoPool = mutex.synchronized {
    if(null == kpool) {
      kpool = KryoPool.withByteArrayOutputStream(guessThreads, new KijiKryoInstantiator)
    }
    kpool
  }

  private def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }
}


class KijiKryoInstantiator extends ScalaKryoInstantiator {
  def apply(kryo: Kryo) {
    val kryo = super.newKryo()
    // k is implicitly converted to a rich kryo
    kryo.addDefaultSerializer(classOf[Schema], new AvroSchemaSerializer)
    kryo.addDefaultSerializer(classOf[SpecificRecord], new AvroSpecificSerializer)
    kryo.addDefaultSerializer(classOf[GenericContainer], new AvroGenericSerializer)
  }

}