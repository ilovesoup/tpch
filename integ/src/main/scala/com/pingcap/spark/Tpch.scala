package com.pingcap.spark
import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ilovesoup1 on 26/07/2017.
  */
abstract class Tpch(val spark: SparkSession, val prop: Properties) {

  protected def tidbQuery: String
  protected def sparkQuery: String
  def testName(): String = getClass.getSimpleName

  def test() = {
    println("================= Query TiSpark =================\n")
    val actual: List[List[Any]] = querySpark()
    println("================= Query TiDB =================\n")
    val baseline: List[List[Any]] = queryTiDB(prop, "tpch", tidbQuery)

    val result = compResult(actual, baseline)
    if (!result) {
      println("================= TiSpark =================\n")
      printResult(actual)
      println("================= TiDB =================\n")
      printResult(baseline)
    }

    println(testName + " result: " + result)
  }


  def compResult(lhs: List[List[Any]], rhs: List[List[Any]]): Boolean = {
    def toDouble(x: Any): Double = x match {
      case d: Double => d
      case d: Float => d.toDouble
      case d: java.math.BigDecimal => d.doubleValue()
      case d: BigDecimal => d.bigDecimal.doubleValue()
    }

    def toInteger(x: Any): Long = x match {
      case d: Long => d
      case d: Integer => d.toLong
      case d: Short => d.toLong
      case d: java.math.BigInteger => d.longValue()
      case d: BigInt => d.bigInteger.longValue()
    }

    def compValue(lhs: Any, rhs: Any): Boolean = lhs match {
        case _: Double | _: Float | _: BigDecimal | _: java.math.BigDecimal =>
          Math.abs(toDouble(lhs) - toDouble(rhs)) < 0.01
        case _: Number | _: BigInt | _: java.math.BigInteger =>
          toInteger(lhs) == toInteger(rhs)
        case _ => lhs == rhs
      }


    def compRow(lhs: List[Any], rhs: List[Any]): Boolean = {
      if (lhs == null && rhs == null) {
        true
      } else if (lhs == null || rhs == null) {
        false
      } else {
        !lhs.zipWithIndex.exists {
          case (value, i) => rhs.length <= i || !compValue(value, rhs(i))
        }
      }
    }

    !lhs.zipWithIndex.exists {
      case (row, i) => rhs.length <= i || !compRow(row, rhs(i))
    }
  }

  def printResult(rowList: List[List[Any]]): Unit = {
    rowList.foreach{
      row => {
        row.foreach{
          value => print(value + " ")
        }
        println("")
      }
    }
  }

  def querySpark(): List[List[Any]] = {
    spark.sql(sparkQuery).collect().map(row => {
      val rowRes = ArrayBuffer.empty[Any]
      for (i <- 0 to row.length - 1) {
        rowRes += row.get(i)
      }
      rowRes.toList
    }).toList
  }

  def queryTiDB(prop: Properties, jdbcDatabase: String, query: String): List[List[Any]] = {
    val jdbcUsername = prop.getProperty("tidbuser")
    val jdbcHostname = prop.getProperty("tidbaddr")
    val jdbcPort = Integer.parseInt(prop.getProperty("tidbport"))

    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}"
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, "")
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)
    val rsMetaData = resultSet.getMetaData();

    val retSet = ArrayBuffer.empty[List[Any]]
    while (resultSet.next()) {
      val row = ArrayBuffer.empty[Any]

      for (i <- 1 to rsMetaData.getColumnCount) {
        row += resultSet.getObject(i)
      }
      retSet += row.toList
    }
    retSet.toList
  }
}
