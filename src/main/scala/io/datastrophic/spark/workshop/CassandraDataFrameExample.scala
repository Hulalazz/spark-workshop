package io.datastrophic.spark.workshop

import java.sql.Timestamp

import com.datastax.spark.connector._
import io.datastrophic.spark.workshop.domain.DFCassandraRowWrapper
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.DateTime

object CassandraDataFrameExample extends App {

   implicit def pimpCassandraRowForDF(row: CassandraRow): DFCassandraRowWrapper = new DFCassandraRowWrapper(row: CassandraRow)

   val conf = new SparkConf(true).setAppName("Spark Cassandra Demo").set("spark.cassandra.connection.host", "sandbox")
   val sc = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)


   def run(sc: SparkContext, sqlContext: SQLContext) {
      import org.apache.spark.sql.functions._
      import sqlContext.implicits._

      val watermark = new Timestamp(new DateTime().minusDays(1).getMillis)

      //read events table
      val events = sc.cassandraTable("demo", "event")
                   .map(_.toEvent)
                   .toDF()

      //read campaign table
      val campaigns = sc.cassandraTable("demo", "campaign")
                      .map(_.toCampaign)
                      .toDF()

      //aggregate raw events after specific date grouped by (campaign, event type)
      val aggregatedEvents =
         events
         .filter($"time" > watermark)
         .select($"campaignId", $"eventType", $"value")
         .groupBy($"campaignId", $"eventType")
         .agg(sum("value") as "value")

      //aggregate campaign rolled-up events before specific date grouped by campaign and type
      val aggregatedCampaigns =
         campaigns
         .filter($"day" < watermark)
         .select($"id".as("campaignId"), $"eventType", $"value")
         .groupBy($"campaignId", $"eventType")
         .agg(sum("value").as("total"))

      //joining raw events with rolled-up and summing values
      val joinedDF =
         aggregatedCampaigns
         .join(aggregatedEvents, List("campaignId", "eventType"))
         .select($"campaignId", $"eventType", $"total" + $"value" as "events")

      //breakdown results by events and sorting by total value
      val totalsWithBreakdown = joinedDF
      .groupBy($"campaignId")
      .agg(sum("events") as "total")
      .join(joinedDF, List("campaignId"))
      .orderBy($"total".desc)
      .take(15)

      //top 10 aggregated raw events
      val eventTotals =
         aggregatedEvents
         .orderBy($"value".desc)
         .take(10)

      //top 10 campaigns by total amount of events
      val campaignTotals =
         aggregatedCampaigns
         .select($"campaignId", $"eventType", $"total")
         .groupBy($"campaignId")
         .agg(sum("total").as("total"))
         .orderBy($"total".desc)
         .take(10)


      val report =
         s"""
            |Campaign report:
            |
            |Top 10 events in real-time store:
            |${eventTotals.mkString("\n")}
            |
            |Top 10 campaigns by total amount of events in rollups
            |${campaignTotals.mkString("\n")}
            |
            |Combined results:
            |${totalsWithBreakdown.mkString("\n")}
         """.stripMargin

      println(report)
   }

   run(sc, sqlContext)
}