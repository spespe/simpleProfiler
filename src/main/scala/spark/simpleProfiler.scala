package spark

import java.util.Date
import java.util.concurrent.TimeUnit
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

    //Inputs
    val db=args(0).trim
    val table=args(1).trim
    val column=args(2).trim
    val cache=args(3).trim

    //Spark configuration
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
      val df= sqlContext.table(db + "." + table).select(col)
      def rddGen(cache:String)= {
        cache.trim.toUpperCase match {
          case "N" => df.rdd
          case _ => df.rdd.persist(StorageLevel.MEMORY_AND_DISK)
        }
      }
      val rdd=rddGen(cache)
      if(rdd.isEmpty()){
        println(db+sep+table+sep+col+sep+"EMPTY"+sep+"EMPTY"+sep+"EMPTY"+sep+"EMPTY"+sep+"EMPTY")
        System.exit(1)}
      val count=rdd.count
      val distinctCount=rdd.distinct.count
      val isDistinct=if(count==distinctCount)"TRUE" else "FALSE"
      val blanks=rdd.filter(x=>x.mkString.isEmpty).count
      val withBlanks=if(blanks==0)"FALSE" else "TRUE"
      val hasNull=rdd.filter(x=>x!=null).isEmpty.toString.toUpperCase
      val maxLength=rdd.filter(x=>x!=null).reduce((a,b)=>if(a.mkString.length>b.mkString.length) a else b).mkString.length
      val minLength=rdd.filter(x=>x!=null).reduce((a,b)=>if(a.mkString.length<b.mkString.length) a else b).mkString.length
      val maxLengthTrimmed=rdd.filter(x=>x!=null).reduce((a,b)=>if(a.mkString.length.trim>b.mkString.length.trim) a else b).mkString.length
      val minLengthTrimmed=rdd.filter(x=>x!=null).reduce((a,b)=>if(a.mkString.length.trim<b.mkString.length.trim) a else b).mkString.length
      rdd.unpersist()
      println(db+sep+table+sep+col+sep+isDistinct+sep+withBlanks+sep+hasNull+sep+maxLength+sep+minLength)
    }

    profiler(db.mkString)(table.mkString)(column.mkString)(cache.mkString)

    //val endTime= new Date
    //val units=TimeUnit.SECONDS
    //val timeTaken=getDateDiff(startTime,endTime,units)

    //println("[END TIME: ]"+endTime)
    //println("[TIME TAKEN IN SECONDS: ]"+timeTaken)

    sc.stop
  }
}
