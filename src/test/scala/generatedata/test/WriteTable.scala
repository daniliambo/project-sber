package generatedata.test

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

trait WriteTable {

  def writeTable(data: Vector[Vector[Any]], structType: StructType, path: String)(implicit sparkSession: SparkSession): Unit = {

    // For implicit conversions like converting RDDs to DataFrames
    val rdd = sparkSession.sparkContext.makeRDD(data.map(v => Row(v: _*)))
    val df = sparkSession.createDataFrame(rdd, structType)

    df.write.option("path", path).saveAsTable("something")
  }

}
