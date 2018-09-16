import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class CustomKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[WeatherRecord])
  }
}
