import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object gfgfd {

  def main(args: Array[String]): Unit = {


//    val str: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
//
//    val taskJson: JSONObject = JSONObject.fromObject(str)
//
//
//    val startAge = ParamUtils.getParam(taskJson, Constants.PARAM_START_AGE)
//
//
//    val conf: SparkConf = new SparkConf().setAppName("SessionStaticAgg").setMaster("local[*]")
//
//    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//
//
//    val unit: RDD[Int] = spark.sparkContext.makeRDD(Array(1,2,3,4,5))


    val ints: Array[Int] = Array(1,2,3,4,5)

    val ints1: Array[Int] = ints.slice(0,ints.length-1)

    val tail: Array[Int] = ints.tail

    tail.foreach(print(_))


  }

}
