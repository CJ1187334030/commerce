import java.{io, lang}
import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Advertising {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Advertising")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(5))


    val brokers: String = ConfigurationManager.config.getString(Constants.KAFKA_BROKES)
    val topics: String = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    //不能加类型
    val stringToSerializable = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group01",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val realtimeDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topics), stringToSerializable)
    )

    val Dstreamvalue: DStream[String] = realtimeDstream.map(_.value())


    //timestamp 	  province 	  city        userid         adid
    //过滤黑名单
    val filterDstream: DStream[String] = Dstreamvalue.transform {

      logRDD =>

        val blacklists: Array[AdBlacklist] = AdBlacklistDAO.findAll()
        val blackArray: Array[Long] = blacklists.map(x => (x.userid))

        logRDD.filter {
          case log =>
            val str: Long = log.split(" ")(3).toLong
            !blackArray.contains(str)
        }
    }


    Dstreamvalue.foreachRDD(rdd => rdd.foreach(println(_)))

    ssc.start()
    ssc.awaitTermination()



  }

}
