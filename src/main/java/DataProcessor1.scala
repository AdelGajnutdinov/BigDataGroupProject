
import nominatim.NominatimAPI
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{MathUtils, NominatimRequestHistory}

object DataProcessor1 {
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

  private val factoriesSchema = StructType(Array(
    StructField("latitude", DoubleType, nullable = false),
    StructField("longitude", DoubleType, nullable = false)))

  var isExceptionHandled = false
  val nominatimRequestHistory: NominatimRequestHistory = new NominatimRequestHistory()

  val getCountryUDF: UserDefinedFunction = udf {
    (latS: Double, lonS: Double) => {
      getCountry(latS, lonS)
    }
  }

  def getMinDistanceToFactoriesOnlyGerman(hdfsBase: String, sparkSession: SparkSession, readFromFile: Boolean): DataFrame = {
    if (!readFromFile) {
      return sparkSession.read.parquet(hdfsBase + "data/dfWithMinDistOnlyGerman/*.parquet")
    }
    val dfSchema1 = readParquet(sparkSession, schema1, hdfsBase + "schema1/*.parquet")
      .select("sensor_id", "lat", "lon")

    val dfSchema2 = readParquet(sparkSession, schema2, hdfsBase + "schema2/*.parquet")
      .select("sensor_id", "lat", "lon")

    val dfSchema3 = readParquet(sparkSession, schema3, hdfsBase + "schema3/*.parquet")
      .select("sensor_id", "lat", "lon")

    val dfDistinctAll = dfSchema1
      .union(dfSchema2)
      .union(dfSchema3)
      .dropDuplicates()

    val dfOnlyGermany = dropNonGermanSensors(dfDistinctAll)

    val factoriesData = sparkSession.read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(factoriesSchema)
      .csv(hdfsBase + "data/factories_full.csv")

    val dfJoined = dfOnlyGermany.crossJoin(factoriesData)

    val dfWithDistance = dfJoined.withColumn("dist",
      MathUtils.euclideanDistance(col("lat"), col("lon"), col("latitude"), col("longitude")))

    dfWithDistance.createOrReplaceTempView("distances")
    val dfWithMinDist = sparkSession.sql("select sensor_id, min(dist) as minDist " +
      "from distances group by sensor_id")

    val dfWithMinDistNotNull = dropNullsOnMinDist(dfWithMinDist)

    val dfWithMinDistOnlyGermany = dfOnlyGermany.join(dfWithMinDistNotNull, Seq("sensor_id"))

    dfWithMinDistOnlyGermany.write
      .parquet(hdfsBase + "data/dfWithMinDistOnlyGerman")
    dfWithMinDistOnlyGermany
  }

  def printCorrelation(dfWithMinDistOnlyGerman: DataFrame, hdfsBase: String, sparkSession: SparkSession): Unit = {
    val dfSchema1 = readParquet(sparkSession, schema1, hdfsBase + "schema1/*.parquet")
      .select("sensor_id", "P1", "P2", "timestamp")

    val dfSchema2 = readParquet(sparkSession, schema2, hdfsBase + "schema2/*.parquet")
      .select("sensor_id", "P1", "P2", "timestamp")

    val dfSchema3 = readParquet(sparkSession, schema3, hdfsBase + "schema3/*.parquet")
      .select("sensor_id", "P1", "P2", "timestamp")

    val dfWithP1P2 = dfSchema1
      .union(dfSchema2)
      .union(dfSchema3)
      .withColumn("HourOfDay", hour(col("timestamp")))

    val dfFiltered = dfWithP1P2.where("P1 < 5000 and P2 < 5000 and (HourOfDay > 5 and HourOfDay < 18)")

    dfFiltered.createOrReplaceTempView("sensors")
    val dfAvgP = sparkSession.sql("select sensor_id, (P1 + P2)/2 as avgP from sensors")

    val dfJoined = dfAvgP.join(dfWithMinDistOnlyGerman, Seq("sensor_id"))
    val dfJoinedDroppedNulls = dropNullsOnMinDistAndAvg(dfJoined)

    getRandomSample(dfJoinedDroppedNulls, 5000).coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ";")
      .format("com.databricks.spark.csv")
      .csv(hdfsBase + "results/forPrinting1")

    val assembler = new VectorAssembler()
      .setHandleInvalid("skip")
      .setInputCols(Array("avgP", "minDist"))
      .setOutputCol("features")

    val output = assembler.transform(dfJoinedDroppedNulls)

    val matrix: DenseMatrix = Correlation
      .corr(output, "features")
      .select("pearson(features)")
      .first().get(0).asInstanceOf[DenseMatrix]
    println(matrix.toString())
  }

  def dropNullsOnMinDist(dfWithMin: DataFrame): DataFrame = {
    dfWithMin.filter(col("minDist").isNotNull)
  }

  def dropNullsOnMinDistAndAvg(dfWithMin: DataFrame): DataFrame = {
    dfWithMin.filter(col("minDist").isNotNull.or(col("avgP").isNotNull))
  }

  def dropRowsWithNulls(df: DataFrame): DataFrame = {
    df.na.drop()
  }

  def dropNonGermanSensors(df: DataFrame): DataFrame = {
    val dfWithCountry = df.withColumn("country", getCountryUDF(col("lat"), col("lon")))
    dfWithCountry.where("country == 'Deutschland' or country == 'error'")
  }

  def getCountry(lat: Double, lon: Double): String = {
    if (isExceptionHandled) return "error"
    val result = nominatimRequestHistory.getCountry(lat, lon)
    if (result == null) {
      Thread.sleep(1000)
      try {
        val nominatim: NominatimAPI = new NominatimAPI(18)
        val address = nominatim.getAddress(lat, lon)
        nominatimRequestHistory.add(lat, lon, address.getCountry)
      }
      catch {
        case _: Exception =>
          println("Got some other kind of exception")
          isExceptionHandled = true
          return "error"
      }
    }
    result
  }

  def readParquet(sparkSession: SparkSession, schema: StructType, filePath: String): DataFrame = {
    sparkSession.read
      .schema(schema)
      .parquet(filePath)
  }

  def getRandomSample(df: DataFrame, n: Int): DataFrame = {
    val count = df.count()
    val howManyToTake = if (count > n) n else count
    df.sample(withReplacement = false, 1.0 * howManyToTake / count).limit(n)
  }
}
