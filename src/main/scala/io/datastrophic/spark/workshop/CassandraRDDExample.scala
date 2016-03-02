package io.datastrophic.spark.workshop

import com.datastax.spark.connector._
import io.datastrophic.spark.workshop.domain.{CassandraRowWrapper, CampaignTotals}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.DateTime

object CassandraRDDExample extends App {

   implicit def pimpCassandraRow(row: CassandraRow): CassandraRowWrapper = new CassandraRowWrapper(row: CassandraRow)

   val conf = new SparkConf(true).setAppName("Spark Cassandra Demo").set("spark.cassandra.connection.host", "sandbox")
   val sc = new SparkContext(conf)

   def run(sc: SparkContext) {

      val watermark = new DateTime().minusDays(1).getMillis

      //aggregate events after specific date by (campaign id, type)
      val events =
         sc.cassandraTable("demo", "event")
         .map(_.toEvent)
         .filter(event => event.time.isAfter(watermark))
         .keyBy(event => (event.campaignId, event.eventType))
         .reduceByKey(_ + _)
         .cache()

      //aggregate campaigns by (id, type)
      val campaigns =
         sc.cassandraTable("demo", "campaign")
         .map(_.toCampaign)
         .filter(campaign => campaign.day.isBefore(watermark))
         .keyBy(campaign => (campaign.id, campaign.eventType))
         .reduceByKey(_ + _)
         .cache()

      //joining raw events with rolled-up and summing values
      val joinedRDD =
         campaigns.join(events)
         .map { case (key, (campaign, event)) => CampaignTotals(campaign, event) }
         .keyBy(_.id)


      //breakdown results by events and sorting by total value
      val totalsWithBreakdown =
         joinedRDD
         .reduceByKey(_ + _)
         .join(joinedRDD)
         .map { case (uuid, (reduced, source)) => (uuid, reduced.value, source.ad_type, source.value) }
         .top(15)(Ordering.by(_._2))
         .map { case (uuid, total, ad_type, eventValue) => s"[$uuid, $total, $ad_type, $eventValue]"}

      //top 10 aggregated raw events
      val eventTotals = events
                        .top(10)(Ordering.by(_._2.value))
                        .map{ case (t, e) => s"$t -> ${e.value}" }

      //top 10 campaigns by total amount of events
      val campaignTotals = campaigns
                           .map(_._2)
                           .keyBy(_.id)
                           .reduceByKey(_ + _)
                           .values
                           .top(10)(Ordering.by(_.value))
                           .map(c => s"campaign: ${c.id} -> total ${c.value}")

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

   run(sc)
}