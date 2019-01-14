package spark

import java.util.Date
import java.util.concurrent.TimeUnit
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Pietro.Speri on 12/01/2019.
  */
object simpleProfiler extends LazyLogging with functions {
  def main(args: Array[String]) {
    //Start time
    val startTime= new Date
    logger.info("[START TIME: ]"+startTime)

    //Inputs
    val db=args(0).trim
    val table=args(1).trim
    val column=args(3).trim
    logger.info("[ARGUMENTS PASSED: ]"+args.foreach(println))

    //Spark configuration
    logger.info("[CONFIGURING SPARK CONTEXT AND SQL CONTEXT]")
    val conf = new org.apache.spark.SparkConf().setAppName("Profiler")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //Silent mode
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    def profiler(db: => String)(table: => String)(col: => String)={
      val sep="|"
      logger.info("[CREATING RDD]")
      val rdd=sqlContext.table(db+"."+table).select(col).rdd.persist(StorageLevel.MEMORY_AND_DISK)
      logger.info("[RDD COUNT]")
      val count=rdd.count
      logger.info("[RDD DISTINCT COUNT]")
      val distinctCount=rdd.distinct.count
      val isDistinct=if(count==distinctCount)"TRUE" else "FALSE"
      logger.info("[BLANKS COUNT]")
      val blanks=rdd.filter(x=>x.mkString.isEmpty).count
      val withBlanks=if(blanks==0)"FALSE" else "TRUE"
      logger.info("[NULLS COUNT]")
      val hasNull=rdd.filter(x=>x!=null).isEmpty.toString.toUpperCase
      logger.info("[MAX LENGTH CHECK]")
      val maxLength=rdd.reduce((a,b)=>if(a.mkString.length>b.mkString.length) a else b).mkString.length
      logger.info("[MIN LENGTH CHECK]")
      val minLength=rdd.reduce((a,b)=>if(a.mkString.length>b.mkString.length) a else b).mkString.length
      logger.info("[UNPERSISTING RDD]")
      rdd.unpersist()
      println(db+sep+table+sep+col+sep+isDistinct+sep+withBlanks+sep+hasNull+sep+maxLength+sep+minLength)
    }

    logger.info("[LAUNCHING THE PROFILER]")
    profiler(db.mkString)(table.mkString)(column.mkString)

    val endTime= new Date
    val units=TimeUnit.SECONDS
    val timeTaken=getDateDiff(startTime,endTime,units)

    logger.info("[END TIME: ]"+endTime)
    logger.info("[TIME TAKEN IN SECONDS: ]"+timeTaken)

    sc.stop
  }
}
