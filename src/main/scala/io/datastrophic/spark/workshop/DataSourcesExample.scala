package io.datastrophic.spark.workshop

import com.datastax.spark.connector._
import com.mongodb.hadoop.{BSONFileInputFormat, MongoInputFormat}
import io.datastrophic.spark.workshop.domain.DFCassandraRowWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.bson.BSONObject

object DataSourcesExample extends App {

   implicit def pimpCassandraRowForDF(row: CassandraRow): DFCassandraRowWrapper = new DFCassandraRowWrapper(row: CassandraRow)

   val conf = new SparkConf(true).setAppName("Spark Cassandra Demo").set("spark.cassandra.connection.host", "sandbox")
   val sc = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)


   def run(sc: SparkContext, sqlContext: SQLContext, entriesToDisplay: Int) {
      //reading json
      val json = sqlContext.read.json("/workshop/events.json")
      json.printSchema()
      json.take(entriesToDisplay).foreach(println)

      //reading parquet
      val parquet = sqlContext.read.parquet("/workshop/events.parquet")
      parquet.printSchema()
      parquet.take(entriesToDisplay).foreach(println)

      //reading from Cassandra
      val cassandraTable = sc.cassandraTable("demo", "event")
      cassandraTable.take(entriesToDisplay).foreach(println)

      //reading from bson
      val bsonConfig = new Configuration()
      bsonConfig.set("mongo.job.input.format", "com.mongodb.hadoop.BSONFileInputFormat")

      sc.newAPIHadoopFile(
         "/workshop/events.bson",
         classOf[BSONFileInputFormat].asSubclass(classOf[org.apache.hadoop.mapreduce.lib.input.FileInputFormat[Object,BSONObject]]),
         classOf[Object],
         classOf[BSONObject],
         bsonConfig)
      .take(entriesToDisplay).foreach(println)

      //reading directly from Mongo
      val mongoConfig = new Configuration()
      mongoConfig.set("mongo.input.uri", "mongodb://mongo:27017/demo.event")

      sc.newAPIHadoopRDD(
         mongoConfig,
         classOf[MongoInputFormat],
         classOf[Object],
         classOf[BSONObject])
      .take(entriesToDisplay).foreach(println)
   }

   run(sc, sqlContext, 5)
}