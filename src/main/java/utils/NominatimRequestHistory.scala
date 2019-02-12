package utils

import java.util

class NominatimRequestHistory() {

  //key: "lat|lon"
  //value "country"
  var history: util.HashMap[String, String] = new util.HashMap[String, String]()

  def getCountry(lat: Double, lon: Double): String = {
    history.get(makeKey(lat, lon))
  }

  def add(lat: Double, lon: Double, country: String): Unit = {
    history.put(makeKey(lat, lon), country)
  }

  private def makeKey(lat: Double, lon: Double): String = {
    lat.toString + "|" + lon.toString
  }

  def parseKey(key: String): List[Double] = {
    key.split("|").map(s => s.toDouble).toList
  }
}
