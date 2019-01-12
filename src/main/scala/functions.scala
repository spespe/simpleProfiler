package p.spark

import java.util.Date
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

trait functions { // to be extended
  //Method used to retrieve the time taken in seconds
  def getDateDiff(date1:Date,date2:Date,timeUnit:TimeUnit)= {
    val diffInSeconds = date2.getTime() - date1.getTime()
    timeUnit.convert(diffInSeconds,TimeUnit.SECONDS)
  }

  //Alternative method to profile all columns
  def profiler2(tablename: => String, dataFrame: => DataFrame)={
    dataFrame.schema.fieldNames.foreach( f => {
      val table = "profiling_"+tablename+f
      dataFrame.groupBy(f).count.sort(desc("count")).limit(10).saveAsTable(table)
    })
  }
}
