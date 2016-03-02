package io.datastrophic.spark.workshop

import com.datastax.spark.connector._
import io.datastrophic.spark.workshop.domain.DFCassandraRowWrapper
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This app demonstrates how Spark Applications could be parametrized during spark-submit.
  * Let's pick up a simple case of dumping Cassandra table to Parquet where source table and
  * target location are parameters for the application.
  *
  * The application could be submitted like that:

   spark-submit --class io.datastrophic.spark.workshop.ParametrizedApplicationExample \
         --master yarn \
         --deploy-mode cluster \
         --num-executors 2 \
         --driver-memory 1g \
         --executor-memory 1g \
         /target/spark-workshop.jar \
         --cassandra-host cassandra
         --keyspace demo
         --table event
         --target-dir /workshop/dumps
  */
object ParametrizedApplicationExample extends {

   case class Config(
      cassandraHost: String = "",
      keyspace: String = "",
      table: String = "",
      targetDir: String = ""
   )

   implicit def pimpCassandraRow(row: CassandraRow): DFCassandraRowWrapper = new DFCassandraRowWrapper(row: CassandraRow)

   def run(config: Config) {
      val conf = new SparkConf(true).setAppName("Spark Cassandra Demo").set("spark.cassandra.connection.host", config.cassandraHost)
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._

      val rdd = sc.cassandraTable(config.keyspace, config.table)

      config.table match {
         case "event" =>
            rdd.map(_.toEvent).toDF().write.parquet(s"${config.targetDir}/event_dump.parquet")
         case "campaign" =>
            rdd.map(_.toCampaign).toDF().write.parquet(s"${config.targetDir}/campaign_dump.parquet")
         case _ => println("Table name does not match!")
      }
   }

   def main(args: Array[String]): Unit = {
      val parser = new scopt.OptionParser[Config]("scopt") {
         head("scopt", "3.x")
         opt[String]('h', "cassandra-host") required() action { (x, c) => c.copy(cassandraHost = x) } text ("cassandra hostname")
         opt[String]('k', "keyspace") required() action { (x, c) => c.copy(keyspace = x) } text ("keyspace name")
         opt[String]('t', "table") required() action { (x, c) => c.copy(table = x) } text ("table name")
         opt[String]('d', "target-dir") required() action { (x, c) => c.copy(targetDir = x) } text ("dump directory")
         help("help") text("prints this usage text")
      }

      parser.parse(args, Config()) map { config =>
         run(config)
      } getOrElse {
         println("Not all program arguments provided, can't continue")
      }
   }
}