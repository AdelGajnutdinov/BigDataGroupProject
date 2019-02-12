import org.apache.spark.sql.{DataFrame, SparkSession}

object Task1 {

//  val hdfsBase = ""
  val hdfsBase = "/user/adel700/"
  val sparkMaster = "local[*]"
  val readMinDistOnlyGermanFromFile = false

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("group-project")
//      .master(sparkMaster)
      .getOrCreate()

    val dfWithMinDistOnlyGerman = minDistProcessing(sparkSession)

    //correlation between minDist, avgP columns
    DataProcessor1.printCorrelation(dfWithMinDistOnlyGerman, hdfsBase, sparkSession)
  }

  def minDistProcessing (sparkSession: SparkSession): DataFrame = {
    val dfWithMinDistanceOnlyGerman = DataProcessor1
      .getMinDistanceToFactoriesOnlyGerman(hdfsBase, sparkSession, readMinDistOnlyGermanFromFile)

    val dfWithMinDistAndCoordinateNotNull = DataProcessor1.dropRowsWithNulls(dfWithMinDistanceOnlyGerman).sort("sensor_id")
    val dfWithMinDistAndCoordinateNotNullNotDuplicate = dfWithMinDistAndCoordinateNotNull.dropDuplicates("sensor_id")
    dfWithMinDistAndCoordinateNotNullNotDuplicate
  }
}
