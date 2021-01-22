import java.util.UUID

import com.sun.deploy.util.ParameterUtil
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.ivy.util.DateUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageConvertStat {



  def main(args: Array[String]): Unit = {

    val taskParam = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskJson: JSONObject = JSONObject.fromObject(taskParam)

    val stringUUID: String = UUID.randomUUID().toString

    val pageConvertStat: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PageConvertStat")


    val spark: SparkSession = SparkSession.builder().config(pageConvertStat).enableHiveSupport().getOrCreate()

    val originalRDD: RDD[UserVisitAction] = getHiveDate(spark, taskJson)

    val sid2ActionRDD: RDD[(String, UserVisitAction)] = originalRDD.map {
      case item => (item.session_id, item)
    }


    //"1,2,3,4,5,6,7"
    val pageFlowStr: String = ParamUtils.getParam(taskJson,Constants.PARAM_TARGET_PAGE_FLOW)

    val pageFlowArray: Array[String] = pageFlowStr.split(",")
    val strings: Array[String] = pageFlowArray.slice(0,pageFlowArray.length-1)

    //1_2，2_3，3_4，4_5，5_6，6_7
    val standardSlice: Array[String] = strings.zip(pageFlowArray.tail).map {
      case (a, b) => a + "_" + b
    }

    val sid2IterableActionRDD: RDD[(String, Iterable[UserVisitAction])] = sid2ActionRDD.groupByKey()


    val page2map1: RDD[(String, Int)] = sid2IterableActionRDD.flatMap {

      case (sid, iterableAction) =>

        val actionsSort: List[UserVisitAction] = iterableAction.toList.sortWith(
          (c1, c2) => DateUtil.parse(c1.action_time).getTime < DateUtil.parse(c2.action_time).getTime
        )

        val longs: List[Long] = actionsSort.map {
          case item => item.page_id
        }

        val tuples: List[(String, Int)] = longs.slice(0, longs.length - 1).zip(longs.tail)
          .map {
            case (a, b) => a + "_" + b
          }.map((_, 1))

        tuples.filter {
          case (a, b) => standardSlice.contains(a)
        }

    }

    val stringToLong: collection.Map[String, Long] = page2map1.countByKey()

    val startPage: String = standardSlice(0)

    val startPageCount: Long = sid2ActionRDD.filter {
      case (sid, action) =>
        action.page_id.toString == startPage
    }.count()


    getPageConvert(spark,stringUUID,standardSlice,startPageCount,stringToLong)

  }


  def getPageConvert(spark: SparkSession,
                     stringUUID: String,
                     standardSlice: Array[String],
                     startPageCount: Long,
                     stringToLong: collection.Map[String, Long]): Unit = {

    val stringToDouble: mutable.HashMap[String, Double] = new mutable.HashMap[String,Double]()

    var lastPageCount: Double = startPageCount.toDouble

    for (pageSplit <- standardSlice){

      val currentPageSplitCount = stringToLong.get(pageSplit).get.toDouble

      val ratio = currentPageSplitCount / lastPageCount

      stringToDouble.put(pageSplit,ratio)

      lastPageCount = currentPageSplitCount

    }

    val str: String = stringToDouble.map {
      case (page, ratio) => page + "=" + ratio
    }.mkString("|")


    val rate: PageSplitConvertRate = PageSplitConvertRate(stringUUID,str)

    val unit: RDD[PageSplitConvertRate] = spark.sparkContext.makeRDD(Array(rate))

    import spark.implicits._

    unit.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","pageConvert")
      .mode(SaveMode.Append)
      .save()

  }


  def getHiveDate(spark: SparkSession, taskJson: JSONObject) = {

    val startDate: String = ParamUtils.getParam(taskJson,Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskJson,Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"'"

    import spark.implicits._
    spark.sql(sql).as[UserVisitAction].rdd

  }


}
