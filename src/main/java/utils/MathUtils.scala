package utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object MathUtils {
  val euclideanDistance: UserDefinedFunction = udf {
    (latS: Double, lonS: Double, latF: Double, lonF: Double) => {
      Math.sqrt(Math.pow(latS - latF, 2) + Math.pow(lonS - lonF, 2))
    }
  }
}
