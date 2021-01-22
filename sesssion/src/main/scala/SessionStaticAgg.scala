import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.codehaus.janino.util.ClassFile.ConstantNameAndTypeInfo

import scala.collection.immutable.StringOps
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SessionStaticAgg {



  def main(args: Array[String]): Unit = {

    val str: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskJson: JSONObject = JSONObject.fromObject(str)

    val stringUUID: String = UUID.randomUUID().toString

    val conf: SparkConf = new SparkConf().setAppName("SessionStaticAgg").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val original_rdd: RDD[UserVisitAction] = getActiomRDD(spark, taskJson)

    //(8665642ca35145dbb8f89ec4738b2f44,UserVisitAction(2020-10-27,94,8665642ca35145dbb8f89ec4738b2f44,8,2020-10-27 7:05:45,null,92,99,null,null,null,null,5))
    val session2rdd: RDD[(String, UserVisitAction)] = original_rdd.map {
      item => (item.session_id, item)
    }


    //rdd[session,iterable[item]]
    val session2Iterable: RDD[(String, Iterable[UserVisitAction])] = session2rdd.groupByKey()

    //(6,sessionid=048a71c375a04190a70b50d171450ba5|searchKeywords=苹果,联想笔记本,保温杯|clickCategoryIds=60,68,81,13|visitLength=3248|stepLength=16|startTime=2020-10-27 03:03:52)
    val fullInfoRDD: RDD[(String, String)] = getFullInfo(session2Iterable, spark)

    val accumlator: SessionStaticAccumlator = new SessionStaticAccumlator
    spark.sparkContext.register(accumlator)

    //过滤符合条件的数据
    val filterRDD: RDD[(String, String)] = getfilterRDD(taskJson, fullInfoRDD, accumlator)

    //！！！！！！！！！！！！！！！注意懒执行，执行才能更新累加器
//    filterRDD.count()

    //需求一：统计各范围dsession占比
//    sessionCountStudio(spark,stringUUID,accumlator.value)

    //需求二：随机抽取session
//    sessionRandomExtract(spark, filterRDD, stringUUID)

    //需求三：Top10热门品类统计
    //3.1过滤符合条件的action
    val filterActionRDD: RDD[(String, UserVisitAction)] = session2rdd.join(filterRDD).map {
      case (sid, (action, filter)) =>
        (sid, action)
    }

    //3.2Main
    val tuples: Array[(SortKey, String)] = top10PopularCategories(spark,stringUUID,filterActionRDD)

    //需求四：Top10热门品类
    top10ActiveSession(spark,stringUUID,filterActionRDD,tuples)

  }

  def top10ActiveSession(spark: SparkSession,
                         stringUUID: String,
                         filterActionRDD: RDD[(String, UserVisitAction)],
                         tuples: Array[(SortKey, String)]): Unit = {

    //方式一：
    //    val value: RDD[(Long, UserVisitAction)] = filterActionRDD.map {
    //      case (sid, action) =>
    //        val l: Long = action.click_category_id
    //        (l, action)
    //    }
    //
    //    val value1: RDD[(Long, String)] = spark.sparkContext.makeRDD(tuples).map {
    //
    //      case (sortKey, info) =>
    //        val cid: Long = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CATEGORY_ID).toLong
    //        (cid, info)
    //    }
    //
    //    value.join(value1).map{
    //      case(cid,(action,countInfo)) =>
    //        val sid: String = action.session_id
    //        (sid,action)
    //    }

    //方式二：使用filter
    val strings: Array[Long] = tuples.map {
      case (sortKey, countInfo) =>
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }

    //过滤出点击过热门品类的action
    val sid2action: RDD[(String, UserVisitAction)] = filterActionRDD.filter {
      case (sid, action) =>
        strings.contains(action.click_category_id)
    }

    //[sid,iterable[action1,actinon2]]
    val sid2actionIterable: RDD[(String, Iterable[UserVisitAction])] = sid2action.groupByKey()

    //[cid,sidxxx=18]
    val cid2sidCount: RDD[(Long, String)] = sid2actionIterable.flatMap {
      case (sid, actionIterable) =>
        val longToLong: mutable.HashMap[Long, Long] = new mutable.HashMap[Long, Long]()

        for (action <- actionIterable) {

          val cid: Long = action.click_category_id
          if (!longToLong.contains(cid))
            longToLong += (cid -> 0)
          longToLong.update(cid, longToLong(cid) + 1)

        }

        //[cid,sidxxx=18]
        for ((cid, count) <- longToLong)
          yield (cid, sid + "=" + count)

    }

    val cid2sidCountgroup: RDD[(Long, Iterable[String])] = cid2sidCount.groupByKey()


    val top10Session: RDD[Top10Session] = cid2sidCountgroup.flatMap {

      case (cid, iterable) =>

        val strings: List[String] = iterable.toList.sortWith((c1, c2) =>
          c1.split("=")(1).toLong > c2.split("=")(1).toLong
        ).take(10)

        val sessions: List[Top10Session] = strings.map {

          case item =>
            val sid: String = item.split("=")(0)
            val count: Long = item.split("=")(1).toLong

            Top10Session(stringUUID, cid, sid, count)
        }
        sessions
    }

    import spark.implicits._
    top10Session.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10Session")
      .mode(SaveMode.Append)
      .save()

  }



  def getClickActionCount(filterActionRDD: RDD[(String, UserVisitAction)]) = {

    val filterClick: RDD[(String, UserVisitAction)] = filterActionRDD.filter(_._2.click_category_id != -1)

    val click2Num: RDD[(Long, Long)] = filterClick.map {

      case (sid, action) => (action.click_category_id, 1L)

    }

    click2Num.reduceByKey(_+_)

  }


  def getOrderActionCount(filterActionRDD: RDD[(String, UserVisitAction)]) = {

    val filterOrder: RDD[(String, UserVisitAction)] = filterActionRDD.filter(_._2.order_category_ids != null)

    val order2Num: RDD[(Long, Long)] = filterOrder.flatMap(_._2.order_category_ids.split(",")).map(x=>(x.toLong,1L))

    order2Num.reduceByKey(_+_)

  }

  def getPayActionCount(filterActionRDD: RDD[(String, UserVisitAction)]) = {

    val filterPay: RDD[(String, UserVisitAction)] = filterActionRDD.filter(_._2.pay_category_ids != null)

    val pay2Num: RDD[(Long, Long)] = filterPay.flatMap(_._2.pay_category_ids.split(",")).map(x=>(x.toLong,1L))

    pay2Num.reduceByKey(_+_)

  }

  def getFullCount(allActionCategoriesRDD: RDD[(Long, Long)],
                   clickCount: RDD[(Long, Long)],
                   orderCount: RDD[(Long, Long)],
                   payCount: RDD[(Long, Long)]) = {


    val cid2CidClick: RDD[(Long, String)] = allActionCategoriesRDD.leftOuterJoin(clickCount).map {

      case (cid, (category, option)) =>

        val clickCount: Long = if (option.isDefined) option.get else 0

        val agginfo: String = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, agginfo)
    }

    val cid2CidClick2order: RDD[(Long, String)] = cid2CidClick.leftOuterJoin(orderCount).map {

      case (cid, (leftinfo, option)) =>
        val orderCount: Long = if (option.isDefined) option.get else 0

        val agginfo = leftinfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, agginfo)

    }


    cid2CidClick2order.leftOuterJoin(payCount).map {

      case (cid, (leftinfo, option)) =>
        val payCount: Long = if (option.isDefined) option.get else 0

        val agginfo = leftinfo + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, agginfo)
    }



  }

  def top10PopularCategories(spark: SparkSession, stringUUID: String, filterActionRDD: RDD[(String, UserVisitAction)]) = {

    var allActionCategoriesRDD: RDD[(Long, Long)] = filterActionRDD.flatMap {

      case (sid, action) =>

        val allActionCategories: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()

        if (action.click_category_id != -1L)
          allActionCategories += ((action.click_category_id, action.click_category_id))
        else if (action.order_category_ids != null) {
          for (order <- action.order_category_ids.split(","))
            allActionCategories += ((order.toLong, order.toLong))
        }
        else if (action.pay_category_ids != null) {
          for (pay <- action.pay_category_ids.split(","))
            allActionCategories += ((pay.toLong, pay.toLong))
        }

        allActionCategories
    }

    allActionCategoriesRDD = allActionCategoriesRDD.distinct()


    //获取点击次数
    val clickCount: RDD[(Long, Long)] = getClickActionCount(filterActionRDD)
    //获取下单次数
    val orderCount: RDD[(Long, Long)] = getOrderActionCount(filterActionRDD)
    //获取支付次数
    val payCount: RDD[(Long, Long)] = getPayActionCount(filterActionRDD)

    //(68,categoryid=68|clickCount=148|orderCount=146|payCount=145)
    val cid2FullCount: RDD[(Long, String)] = getFullCount(allActionCategoriesRDD,clickCount,orderCount,payCount)


    // 实现自定义二次排序
    val sort2Count: RDD[(SortKey, String)] = cid2FullCount.map {
      case (cid, info) =>
        val clickCount: Long = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount: Long = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount: Long = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val key: SortKey = SortKey(clickCount, orderCount, payCount)

        (key, info)

    }

    val tuples: Array[(SortKey, String)] = sort2Count.sortByKey(false).take(10)

    val top10CategoryRDD: RDD[Top10Category] = spark.sparkContext.makeRDD(tuples).map {
      case (key, info) =>
        val cid: Long = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CATEGORY_ID).toLong

        val count: Long = key.clickCount
        val order: Long = key.order
        val pay: Long = key.pay

        Top10Category(stringUUID, cid, count, order, pay)

    }


    import spark.implicits._

    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10Category")
      .mode(SaveMode.Append)
      .save()


    tuples
  }



  def generateRandomIndexList(extractPerDay: Int,
                              daysum: Long,
                              hourCount: mutable.HashMap[String, Long],
                              stringToInts: mutable.HashMap[String, ListBuffer[Int]]) = {

    for ((hour, count) <- hourCount) {


      //每个小时抽取的数量
      //!!!!注意加toDouble，/后<1 => int(0)
      var extractPerhour: Int = ((count / daysum.toDouble) * extractPerDay).toInt

      if (extractPerhour > count)
        extractPerhour = count.toInt

      val random: Random = new Random()

      stringToInts.get(hour) match {

        case None => stringToInts(hour) = new ListBuffer[Int]
          for (i <- 0 until extractPerhour) {
            var index: Int = random.nextInt(count.toInt)
            while (stringToInts(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }

            stringToInts(hour).append(index)
          }

        case Some(listBuffer) =>
          for (i <- 0 until extractPerhour) {
            var index: Int = random.nextInt(count.toInt)
            while (stringToInts(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            stringToInts(hour).append(index)
          }
      }
    }
  }


  def sessionRandomExtract(spark: SparkSession,
                           filterRDD: RDD[(String, String)],
                           stringUUID: String): Unit = {

    //[Date_Hour,info]
    val datetime2FullinfoRDD: RDD[(String, String)] = filterRDD.map {
      case (sid, fullinfo) =>
        val start_time: String = StringUtils.getFieldFromConcatString(fullinfo, "\\|", Constants.FIELD_START_TIME)
        //yyyy-MM-dd_HH
        val str: String = DateUtils.getDateHour(start_time)

        (str, fullinfo)
    }

    //[Date_Hour,22(次數)]
    val datehour2Count: collection.Map[String, Long] = datetime2FullinfoRDD.countByKey()

    //[date,[houe,count]]
    val date2Hour2Count: mutable.HashMap[String, mutable.HashMap[String, Long]] = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((datehour, count) <- datehour2Count) {

      val strings: Array[String] = datehour.split("_")
      val date: String = strings(0)
      val hour: String = strings(1)

      //map[date,[hour,count]]
      date2Hour2Count.get(date) match {
        case None => date2Hour2Count(date) = new mutable.HashMap[String, Long]()
          date2Hour2Count(date) += (hour -> count)
        case Some(map) => date2Hour2Count(date) += (hour -> count)

      }
    }

    //平均一天抽取数据
    val extractPerDay: Int = 100 / date2Hour2Count.size

    //map[date,[hour,List[index(1,2,3,4)]]]
    val date2hour2extractList: mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]] =
      new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    //一天一共有多少数据
    for ((date, hourCount) <- date2Hour2Count) {

      val daysum: Long = hourCount.values.sum

      date2hour2extractList.get(date) match {

        case None => date2hour2extractList(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, daysum, hourCount, date2hour2extractList(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, daysum, hourCount, date2hour2extractList(date))

      }

      //broadcast
      val date2hour2extractListBd: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]]
      = spark.sparkContext.broadcast(date2hour2extractList)


      val datetime2GroupRDD: RDD[(String, Iterable[String])] = datetime2FullinfoRDD.groupByKey()

      val SessionRandomExtractRDD: RDD[SessionRandomExtract] = datetime2GroupRDD.flatMap {

        case (datetime, iterableInfo) =>

          val strings: mutable.ArrayOps[String] = datetime.split("_")
          val date: StringOps = strings(0)
          val hour: StringOps = strings(1)

          //bd =》 value =》 map =》 values =》 iterable
          val indexList: ListBuffer[Int] = date2hour2extractListBd.value.get(date).get(hour)

          val extractsBuffer: ArrayBuffer[SessionRandomExtract] = new ArrayBuffer[SessionRandomExtract]()

          var index = 0

          for (info <- iterableInfo) {
            if (indexList.contains(index)) {

              val sid = StringUtils.getFieldFromConcatString(info, "\\|"
                ,Constants.FIELD_SESSION_ID)
              val startTime = StringUtils.getFieldFromConcatString(info, "\\|"
                ,Constants.FIELD_START_TIME)
              val searchKW = StringUtils.getFieldFromConcatString(info, "\\|"
                ,Constants.FIELD_SEARCH_KEYWORDS)
              val clickCAtegory = StringUtils.getFieldFromConcatString(info, "\\|"
                ,Constants.FIELD_CLICK_CATEGORY_IDS)

              val extract: SessionRandomExtract = SessionRandomExtract(stringUUID, sid, startTime, searchKW, clickCAtegory)
              extractsBuffer += extract
            }
            index += 1
          }
          extractsBuffer
      }

      import spark.implicits._

      SessionRandomExtractRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_random_extract")
        .mode(SaveMode.Append)
        .save()
    }
  }



  def sessionCountStudio(spark: SparkSession,
                         stringUUID: String,
                         value: mutable.HashMap[String, Int]) = {

    //session总计
    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    //各个session数量
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)


    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)


    // 将统计结果封装为Domain对象
    val sessionAggrStat = SessionAggrStat(stringUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)


    import spark.implicits._
    val sessionAggrStatRDD = spark.sparkContext.makeRDD(Array(sessionAggrStat))

    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","session_aggr_stat")
      .mode(SaveMode.Append)
      .save()

  }



  def getfilterRDD(taskJson: JSONObject, FullInfoRDD: RDD[(String, String)], accumlator: SessionStaticAccumlator) = {

    val startAge = ParamUtils.getParam(taskJson, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskJson, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskJson, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskJson, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskJson, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskJson, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskJson, Constants.PARAM_CATEGORY_IDS)

    var filterinfo = (if (startAge != null) Constants.PARAM_END_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterinfo.endsWith("\\|"))
      filterinfo = filterinfo.substring(0, filterinfo.length - 1)

    val sessionId2FilterRDD: RDD[(String, String)] = FullInfoRDD.filter {

      case (sid, info) =>

        var flag = true

        if (!ValidUtils.between(info, Constants.FIELD_AGE, filterinfo, Constants.PARAM_START_AGE,
          Constants.PARAM_END_AGE))
          flag = false

        if (!ValidUtils.equal(info, Constants.FIELD_PROFESSIONAL, filterinfo, Constants.PARAM_PROFESSIONALS))
          flag = false

        if (!ValidUtils.equal(info, Constants.FIELD_CITY, filterinfo, Constants.PARAM_CITIES))
          flag = false

        if (!ValidUtils.equal(info, Constants.FIELD_SEX, filterinfo, Constants.PARAM_SEX))
          flag = false

        if (!ValidUtils.in(info, Constants.FIELD_SEARCH_KEYWORDS, filterinfo, Constants.PARAM_KEYWORDS))
          flag = false

        if (!ValidUtils.in(info, Constants.FIELD_CLICK_CATEGORY_IDS, filterinfo, Constants.PARAM_CATEGORY_IDS))
          flag = false

        if (flag) {

          accumlator.add(Constants.SESSION_COUNT)

          def caculateVisitLength(visitLength: Long) = {
            if (visitLength >= 1 && visitLength <= 3) {
              accumlator.add(Constants.TIME_PERIOD_1s_3s);
            } else if (visitLength >= 4 && visitLength <= 6) {
              accumlator.add(Constants.TIME_PERIOD_4s_6s);
            } else if (visitLength >= 7 && visitLength <= 9) {
              accumlator.add(Constants.TIME_PERIOD_7s_9s);
            } else if (visitLength >= 10 && visitLength <= 30) {
              accumlator.add(Constants.TIME_PERIOD_10s_30s);
            } else if (visitLength > 30 && visitLength <= 60) {
              accumlator.add(Constants.TIME_PERIOD_30s_60s);
            } else if (visitLength > 60 && visitLength <= 180) {
              accumlator.add(Constants.TIME_PERIOD_1m_3m);
            } else if (visitLength > 180 && visitLength <= 600) {
              accumlator.add(Constants.TIME_PERIOD_3m_10m);
            } else if (visitLength > 600 && visitLength <= 1800) {
              accumlator.add(Constants.TIME_PERIOD_10m_30m);
            } else if (visitLength > 1800) {
              accumlator.add(Constants.TIME_PERIOD_30m);
            }
          }


          def calculateStepLength(stepLength: Long) {
            if (stepLength >= 1 && stepLength <= 3) {
              accumlator.add(Constants.STEP_PERIOD_1_3);
            } else if (stepLength >= 4 && stepLength <= 6) {
              accumlator.add(Constants.STEP_PERIOD_4_6);
            } else if (stepLength >= 7 && stepLength <= 9) {
              accumlator.add(Constants.STEP_PERIOD_7_9);
            } else if (stepLength >= 10 && stepLength <= 30) {
              accumlator.add(Constants.STEP_PERIOD_10_30);
            } else if (stepLength > 30 && stepLength <= 60) {
              accumlator.add(Constants.STEP_PERIOD_30_60);
            } else if (stepLength > 60) {
              accumlator.add(Constants.STEP_PERIOD_60);
            }
          }


          //获取总的布长时长
          val visitLength: Long = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength: Long = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          caculateVisitLength(visitLength)
          calculateStepLength(stepLength)
        }
        flag
    }
    sessionId2FilterRDD
  }


  def getFullInfo(session2Iterable: RDD[(String, Iterable[UserVisitAction])],sparkSession: SparkSession) ={

    val user2agginfo: RDD[(Long, String)] = session2Iterable.map {
      case (sid, interable) => {

        var userId = -1L
        val sessionId = sid

        val searchKeyWords = new StringBuffer("")
        val categories = new StringBuffer("")

        var stepLength = 0

        var startTime: Date = null
        var endTime: Date = null
        var visitLength = -1L

        for (item <- interable) {
          if (userId == -1L)
            userId = item.user_id

          val action_date: Date = DateUtils.parseTime(item.action_time)

          if (startTime == null || startTime.after(action_date))
            startTime = action_date

          if (endTime == null || endTime.before(action_date))
            endTime = action_date

          stepLength += 1

          val searchKeyWord = item.search_keyword
          val category = item.click_category_id

          if (StringUtils.isNotEmpty(searchKeyWord) && !searchKeyWords.toString.contains(searchKeyWord))
            searchKeyWords.append(searchKeyWord + ",")

          if (category != -1L && !categories.toString.contains(category))
            categories.append(category + ",")

        }

        visitLength = (endTime.getTime - startTime.getTime) / 1000

        val searchKeyWords_trim: String = StringUtils.trimComma(searchKeyWords.toString)
        val categories_trim: String = StringUtils.trimComma(categories.toString)

        val aggInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords_trim + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + categories_trim + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggInfo)

      }

    }

    val sql = "select * from user_info"
    import sparkSession.implicits._
    val user2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(x => (x.user_id,x))

    user2agginfo.join(user2InfoRDD).map{
      case(user,(agginfo,userinfo)) =>
        val age: Int = userinfo.age
        val professional: String = userinfo.professional
        val sex: String = userinfo.sex
        val city: String = userinfo.city

        val fullInfo: String = agginfo + "|" + Constants.FIELD_AGE + "=" + age + "," +
        Constants.FIELD_PROFESSIONAL + "=" + professional + "," +
        Constants.FIELD_SEX + "=" + sex + "," +
        Constants.FIELD_CITY + "=" + city

        val s_id: String = StringUtils.getFieldFromConcatString(agginfo,"\\|",Constants.FIELD_SESSION_ID)


        (s_id,fullInfo)

    }

  }



  def getActiomRDD(sparkSession: SparkSession,JSONObject: JSONObject) ={


    val start_date: String = ParamUtils.getParam(JSONObject,Constants.PARAM_START_DATE)
    val end_date: String = ParamUtils.getParam(JSONObject,Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date <= '"+end_date+"' and date >= '"+start_date+"'"

    import sparkSession.implicits._

    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql).as[UserVisitAction].rdd

    rdd

  }

}
