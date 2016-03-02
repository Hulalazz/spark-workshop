package io.datastrophic.spark.workshop

import java.sql.Timestamp
import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.mongodb.BasicDBObject
import com.mongodb.hadoop.{BSONFileOutputFormat, MongoOutputFormat}
import io.datastrophic.spark.workshop.domain.{Campaign, Event, DFCampaign, DFEvent}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

object SampleDataWriter extends App {
   val conf = new SparkConf(true).setAppName("Spark Cassandra Demo").set("spark.cassandra.connection.host", "sandbox")
   val sc = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)

   def writeBson(path: String, rdd: RDD[(UUID, BSONObject)]) = {
      rdd
      .saveAsNewAPIHadoopFile(
         path,
         classOf[UUID],
         classOf[BSONObject],
         classOf[BSONFileOutputFormat[Object,BSONObject]]
      )
   }

   def writeToMongo(uri: String, rdd: RDD[(UUID, BSONObject)]) = {
      val mongoConfig = new Configuration()
      mongoConfig.set("mongo.output.uri", uri)

      rdd.saveAsNewAPIHadoopFile(
         "",
         classOf[Object],
         classOf[BSONObject],
         classOf[MongoOutputFormat[Object,BSONObject]],
         mongoConfig
      )
   }

   def generateData(sc: SparkContext, sqlContext: SQLContext) {

      import Converter._
      import DDLWriter._
      import SampleDataGenerator._
      import sqlContext.implicits._

      val keyspace = "demo"

      createCassandraSchema(sc.getConf, keyspace)

      val campaigns = sc.parallelize(generateCampaigns(amount = 5, spanDays = 20))
      val events = campaigns.flatMap(campaign => generateEventsForCampaign(campaign.id, 500))

      //creating DataFrames and flattening unsupported types
      val campaignsDF = campaigns.map(toDFCampaign).toDF()
      val eventsDF = events.map(toDFEvent).toDF()

      //writing Json to HDFS
      campaignsDF.write.json("/workshop/campaigns.json")
      eventsDF.write.json("/workshop/events.json")

      //writing Parquet to HDFS
      campaignsDF.write.parquet("/workshop/campaigns.parquet")
      eventsDF.write.parquet("/workshop/events.parquet")
      //writing to Cassandra
      events.saveToCassandra("demo", "event")
      campaigns.saveToCassandra("demo", "campaign")

      //creating paired(key-value) RDDs of BSONObjects
      val bsonCampaigns = campaigns.map(campaign => (UUID.randomUUID(), campaignToBSON(campaign)))
      val bsonEvents = events.map(event => (UUID.randomUUID(), eventToBSON(event)))

      writeBson("/workshop/campaigns.bson", bsonCampaigns)
      writeBson("/workshop/events.bson", bsonEvents)

      writeToMongo("mongodb://mongo:27017/demo.campaign", bsonCampaigns)
      writeToMongo("mongodb://mongo:27017/demo.event", bsonEvents)
  }

   generateData(sc, sqlContext)
}

object DDLWriter {
   def createCassandraSchema(conf: SparkConf, keyspace: String) = {
      CassandraConnector(conf).withSessionDo { session =>
         session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };")

         session.execute(s"""
                    CREATE TABLE IF NOT EXISTS $keyspace.event (
                     id uuid,
                     campaign_id uuid,
                     event_type text,
                     value bigint,
                     time timestamp,
                     PRIMARY KEY((id, event_type), campaign_id, time)
                    );
                    """.stripMargin)

         session.execute(s"""
                 CREATE TABLE IF NOT EXISTS $keyspace.campaign (
                   id uuid,
                   event_type text,
                   day timestamp,
                   value bigint,
                   PRIMARY KEY((id, event_type), day)
                 );
                """.stripMargin)
      }
   }
}

object Converter {
   import org.json4s._
   import org.json4s.jackson.Serialization
   import org.json4s.jackson.Serialization.write
   implicit val formats = Serialization.formats(NoTypeHints)

   def campaignToBSON(c: Campaign): BSONObject = BasicDBObject.parse(write(c).toString)

   def eventToBSON(e: Event): BSONObject = BasicDBObject.parse(write(e).toString)

   def toDFCampaign(c: Campaign): DFCampaign = DFCampaign(
      id = c.id.toString,
      eventType = c.eventType,
      day = new Timestamp(c.day.getMillis),
      value = c.value
   )

   def toDFEvent(e: Event): DFEvent = DFEvent(
      id = e.id.toString,
      campaignId = e.campaignId.toString,
      eventType = e.eventType,
      value = e.value,
      time = new Timestamp(e.time.getMillis)
   )
}