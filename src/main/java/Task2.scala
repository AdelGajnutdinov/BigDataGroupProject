
import Task1.{hdfsBase, readMinDistOnlyGermanFromFile}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task2 {

//  val hdfsBase = ""
  val hdfsBase = "/user/adel700/"
  val sparkMaster = "local[*]"
  val readElevationAndPopulationFromFile = true

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("group-project")
//      .master(sparkMaster)
      .getOrCreate()

    distributed(sparkSession)
  }

  def distributed(sparkSession: SparkSession): Unit = {
    val dfOnlyGermanSensors = DataProcessor1
      .getMinDistanceToFactoriesOnlyGerman(hdfsBase, sparkSession, readMinDistOnlyGermanFromFile)

    var dfCities500OnlyGerman = DataProcessor2.getCities500OnlyGermanFromFile(hdfsBase, sparkSession)

    var dfSensorsWithEPD = DataProcessor2.
        getElevationAndPopulation(hdfsBase, sparkSession,
          dfOnlyGermanSensors, dfCities500OnlyGerman, readElevationAndPopulationFromFile)
    DataProcessor2.printCorrelation(dfEPDFiltered, hdfsBase, sparkSession)
  }


}
