
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.MathUtils

object DataProcessor2 {
  private val baseSchema = StructType(Array(
    StructField("sensor_id", IntegerType),
    StructField("sensor_type", StringType),
    StructField("location", IntegerType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType),
    StructField("timestamp", StringType)
  ))

  //  schema1 for sds011,ppd42ns
  private val schema1 = baseSchema
    .add(StructField("P1", DoubleType))
    .add(StructField("durP1", StringType))
    .add(StructField("ratioP1", StringType))
    .add(StructField("P2", DoubleType))
    .add(StructField("durP2", StringType))
    .add(StructField("ratioP2", StringType))

  //  schema2 for hpm
  private val schema2 = baseSchema
    .add(StructField("P1", DoubleType))
    .add(StructField("P2", DoubleType))

  //  schmea3 for pms3003,5003,7003
  private val schema3 = schema2
    .add(StructField("P0", DoubleType))

  val cities500Schema = StructType(Array(
    StructField("geonameid", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("asciiname", StringType, nullable = true),
    StructField("alternatenames", StringType, nullable = true),
    StructField("latitude", DoubleType, nullable = false),
    StructField("longitude", DoubleType, nullable = false),
    StructField("feature_class", StringType, nullable = true),
    StructField("feature_code", StringType, nullable = true),
    StructField("country_code", StringType, nullable = false),
    StructField("cc2", StringType, nullable = true),
    StructField("admin1_code", StringType, nullable = true),
    StructField("admin2_code", StringType, nullable = true),
    StructField("admin3_code", StringType, nullable = true),
    StructField("admin4_code", StringType, nullable = true),
    StructField("population", LongType, nullable = false),
    StructField("elevation", IntegerType, nullable = true),
    StructField("dem", IntegerType, nullable = false),
    StructField("timezone", StringType, nullable = true),
    StructField("modification_date", StringType, nullable = true)))

  val elevationAndPopulationSchema = StructType(Array(
    StructField("sensor_id", IntegerType, nullable = false),
    StructField("lat", DoubleType, nullable = false),
    StructField("lon", DoubleType, nullable = false),
    StructField("geonameid", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("asciiname", StringType, nullable = true),
    StructField("alternatenames", StringType, nullable = true),
    StructField("latitude", DoubleType, nullable = false),
    StructField("longitude", DoubleType, nullable = false),
    StructField("population", LongType, nullable = false),
    StructField("elevation", IntegerType, nullable = false)))

  def getCities500OnlyGermanFromFile(hdfsBase: String, sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(cities500Schema)
      .csv(hdfsBase + "data/cities500.txt")
      .where("country_code == 'DE'")
  }

  def getElevationAndPopulation(hdfsBase: String,
                                sparkSession: SparkSession,
                                dfOnlyGermanSensors: DataFrame,
                                cities500OnlyGerman: DataFrame,
                                readFromFile: Boolean): DataFrame = {
    if (!readFromFile) {
      return sparkSession.read.parquet(hdfsBase + "data/dfWithElevationAndPopulation/*.parquet")
    }
    val dfOnlyGermanSensorsSelected = dfOnlyGermanSensors
      .select("sensor_id", "lat", "lon")

    val cities500DataOnlyGermanySelected = cities500OnlyGerman
      .select("geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude", "population", "dem")

    val dfJoined = dfOnlyGermanSensorsSelected.crossJoin(cities500DataOnlyGermanySelected)

    val dfWithDistance = dfJoined.withColumn("dist",
      MathUtils.euclideanDistance(col("lat"), col("lon"), col("latitude"), col("longitude")))

    dfWithDistance.createOrReplaceTempView("distances")
    val dfWithMinDistanceToCity = sparkSession
      .sql("SELECT a.* FROM distances a LEFT OUTER JOIN distances b ON " +
        "a.sensor_id = b.sensor_id AND a.dist > b.dist WHERE b.sensor_id IS NULL")
      .withColumnRenamed("dist", "minDist")
      .withColumnRenamed("dem", "elevation")

    val dfEPDFiltered = dfWithMinDistanceToCity.where("minDist < 0.05")

    val dfWithElevationAndPopulation = dfEPDFiltered.select("sensor_id", "lat", "lon",
      "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude", "population", "elevation")

    dfWithElevationAndPopulation
      .write
      .parquet(hdfsBase + "data/dfWithElevationAndPopulation")
    dfWithElevationAndPopulation
  }

  def printCorrelation(dfElevationAndPopulation: DataFrame, hdfsBase: String, sparkSession: SparkSession): Unit = {
    val dfElevationAndPopulationSelected = dfElevationAndPopulation
      .select("sensor_id", "population", "elevation")

    val dfSchema1 = readParquet(sparkSession, schema1, hdfsBase + "schema1/*.parquet")
      .select("sensor_id", "P1", "P2")

    val dfSchema2 = readParquet(sparkSession, schema2, hdfsBase + "schema2/*.parquet")
      .select("sensor_id", "P1", "P2")

    val dfSchema3 = readParquet(sparkSession, schema3, hdfsBase + "schema3/*.parquet")
      .select("sensor_id", "P1", "P2")

    val dfWithP1P2 = dfSchema1
      .union(dfSchema2)
      .union(dfSchema3)

    val dfWithNotBigP1P2 = dfWithP1P2.where("P1 < 5000 and P2 < 5000")

    dfWithNotBigP1P2.createOrReplaceTempView("sensors")
    val dfAvgP = sparkSession.sql("select sensor_id, (P1 + P2)/2 as avgP from sensors")

    val dfJoined = dfAvgP.join(dfElevationAndPopulationSelected, Seq("sensor_id"))
    val dfJoinedDroppedNulls = dropNulls(dfJoined)

    val dfGrouppedJoined = dfJoinedDroppedNulls.join(dfElevationAndPopulationSelected, Seq("geonameid"))
      .select("avgP", "population", "elevation")
      .dropDuplicates()

    DataProcessor1.getRandomSample(dfGrouppedJoined, 5000).coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ";")
      .format("com.databricks.spark.csv")
      .csv(hdfsBase + "results/forPrinting2")

    val assembler = new VectorAssembler()
      .setHandleInvalid("skip")
      .setInputCols(Array("avgP", "population", "elevation"))
      .setOutputCol("features")

    val output = assembler.transform(dfGrouppedJoined)

    val matrix: DenseMatrix = Correlation
      .corr(output, "features")
      .select("pearson(features)")
      .first().get(0).asInstanceOf[DenseMatrix]
    println(matrix.toString())
  }

  def dropNulls(df: DataFrame): DataFrame = {
    var result = df.filter(col("population").isNotNull)
    result = result.filter(col("elevation").isNotNull)
    result.filter(col("avgP").isNotNull)
  }

  private def readParquet(sparkSession: SparkSession, schema: StructType, filePath: String): DataFrame = {
    sparkSession
      .read
      .schema(schema)
      .parquet(filePath)
  }
}
