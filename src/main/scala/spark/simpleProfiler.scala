package spark

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.rdd
//import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Pietro.Speri on 12/01/2019.
  */
object profiling {
  def main(args: Array[String]) {
    //Start time
    val startTime= new Date
    println("[START TIME: ]"+startTime)

    //Inputs
    val db=args(0).trim
    val table=args(1).trim
    val column=args(2).trim
    val cache=args(3).trim
    args.foreach(x=>println("ARGUMENT: "+x))

    //Spark configuration
    println("[CONFIGURING SPARK CONTEXT AND SQL CONTEXT]")
    val conf = new org.apache.spark.SparkConf().setAppName("Profiler")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //Silent mode
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    //def getDateDiff(date1:Date,date2:Date,timeUnit:TimeUnit)= {
    //  val diffInSeconds = date2.getTime() - date1.getTime()
    //  timeUnit.convert(diffInSeconds,TimeUnit.SECONDS)
    //}

    def profiler(db: => String)(table: => String)(col: => String)(cache: =>String)={
      val sep="|"
      println("[CREATING RDD]")
      def rddGen(cache:String)= {
        cache.trim.toUpperCase match {
          case "N" => sqlContext.table(db + "." + table).select(col).rdd
          case _ => sqlContext.table(db + "." + table).select(col).rdd.persist(StorageLevel.MEMORY_AND_DISK)
        }
      }
      val rdd=rddGen(cache)
      println("[RDD COUNT]")
      val count=rdd.count
      println("[RDD DISTINCT COUNT]")
      val distinctCount=rdd.distinct.count
      val isDistinct=if(count==distinctCount)"TRUE" else "FALSE"
      println("[BLANKS COUNT]")
      val blanks=rdd.filter(x=>x.mkString.isEmpty).count
      val withBlanks=if(blanks==0)"FALSE" else "TRUE"
      println("[NULLS COUNT]")
      val hasNull=rdd.filter(x=>x!=null).isEmpty.toString.toUpperCase
      println("[MAX LENGTH CHECK]")
      val maxLength=rdd.reduce((a,b)=>if(a.mkString.length>b.mkString.length) a else b).mkString.length
      println("[MIN LENGTH CHECK]")
      val minLength=rdd.reduce((a,b)=>if(a.mkString.length<b.mkString.length) a else b).mkString.length
      println("[UNPERSISTING RDD]")
      rdd.unpersist()
      db+sep+table+sep+col+sep+isDistinct+sep+withBlanks+sep+hasNull+sep+maxLength+sep+minLength
    }

    println("[LAUNCHING THE PROFILER]")
    profiler(db.mkString)(table.mkString)(column.mkString)(cache.mkString)

    //val endTime= new Date
    //val units=TimeUnit.SECONDS
    //val timeTaken=getDateDiff(startTime,endTime,units)

    //println("[END TIME: ]"+endTime)
    //println("[TIME TAKEN IN SECONDS: ]"+timeTaken)

    sc.stop
  }
}
