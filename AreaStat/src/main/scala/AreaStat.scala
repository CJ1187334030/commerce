import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AreaStat {



  def main(args: Array[String]): Unit = {

    val taskParam = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskJson: JSONObject = JSONObject.fromObject(taskParam)

    val stringUUID: String = UUID.randomUUID().toString

    val pageConvertStat: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PageConvertStat")
    val spark: SparkSession = SparkSession.builder().config(pageConvertStat).enableHiveSupport().getOrCreate()


    val cid2pid: RDD[(Long, Long)] = getHiveDate(spark, taskJson)

    val tuples = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    val cid2infoRDD: RDD[(Long, CityAreaInfo)] = spark.sparkContext.makeRDD(tuples).map {
      case (cid, city, area) => (cid, CityAreaInfo(cid, city, area))
    }


    getAreaPidInfo(spark,cid2pid,cid2infoRDD)

    spark.udf.register("concat_cid_cName",(v1:Long,v2:String,split:String) => v1+split+v2)
    spark.udf.register("group_concat_distinct", new GroupConcatDistinct)

    getCidPidCount(spark)

    spark.udf.register("parse_json",(a:String,b:String) => {
      val nObject: JSONObject = JSONObject.fromObject(a)
      nObject.getString(b)
    })

    getCidPidProCount(spark)

    getFinalCount(stringUUID,spark)

    spark.sql("select * from final_count").show()

  }


  //|area|area_level|pid|cityInfos|click_count|product_name|product_status|
  def getFinalCount(stringUUID: String, spark: SparkSession) = {

    val sql: String = "select area,area_level,pid,cityInfos,click_count click_count,product_name,product_status from (" +
      "select area," +
      "case " +
      "when area='华北' or area='华东' then 'A' " +
      "when area='华南' or area='华中' then 'B' " +
      "when area='西北' or area='西南' then 'C' " +
      "else 'D' " +
      "end area_level," +
      "pid,cityInfos,pid_count click_count,product_name,product_status," +
      "rank() over(partition by area order by pid_count desc) rk from cip_pid_pro_count) where rk <= 3"

    spark.sql(sql).createOrReplaceTempView("final_count")

    val value: RDD[AreaTop3Product] = spark.sql(sql).rdd.map {

      case row =>
        AreaTop3Product(stringUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
          row.getAs[Long]("pid"), row.getAs[String]("cityInfos"), row.getAs[Long]("click_count"),
          row.getAs[String]("product_name"), row.getAs[String]("product_status"))
    }

    import spark.implicits._
    value.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","AreaStat")
      .mode(SaveMode.Append)
      .save()

  }


  def getCidPidProCount(spark: SparkSession) = {

    //area|pid|pid_count|cityInfos|product_name|product_status|
    //'product_status' 非字段名称加 ‘’
    //product_info(product_id,product_name,extend_info)
    val sql: String = "select cpc.area,cpc.pid,cpc.pid_count,cpc.cityInfos,pi.product_name," +
      "if(parse_json(extend_info,'product_status') = '1','Self','Third Paty') product_status " +
      "from Cid_Pid_Count cpc join product_info pi on cpc.pid = pi.product_id"
    spark.sql(sql).createOrReplaceTempView("cip_pid_pro_count")

  }


  //|area|pid|pid_count|cityInfos|
  def getCidPidCount(spark: SparkSession) = {

    spark.sql("select area,pid,count(*) pid_count,group_concat_distinct(concat_cid_cName(cid,cName,':')) cityInfos from temp_info group by area,pid")
      .createOrReplaceTempView("Cid_Pid_Count")

  }


  def getAreaPidInfo(spark: SparkSession,
                     cid2pid: RDD[(Long, Long)],
                     cid2infoRDD: RDD[(Long, CityAreaInfo)]): Unit = {

    val info: RDD[(Long, String, String, Long)] = cid2pid.join(cid2infoRDD).map {

      case (cid, (pid, areaInfo)) =>
        (cid, areaInfo.cityName, areaInfo.area, pid)

    }

    import spark.implicits._

    info.toDF("cid","cName","area","pid").createOrReplaceTempView("temp_info")

  }


  def getHiveDate(spark: SparkSession, taskJson: JSONObject) = {

    val startDate: String = ParamUtils.getParam(taskJson,Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskJson,Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"' and click_product_id != -1"

    import spark.implicits._
    spark.sql(sql).as[UserVisitAction].rdd.map{
      case item => (item.city_id,item.click_product_id)
    }

  }



}
