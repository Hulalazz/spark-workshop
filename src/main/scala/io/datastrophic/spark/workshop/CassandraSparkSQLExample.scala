package io.datastrophic.spark.workshop

import java.sql.Timestamp

import com.datastax.spark.connector._
import io.datastrophic.spark.workshop.domain.DFCassandraRowWrapper
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.DateTime

object CassandraSparkSQLExample extends App {

   implicit def pimpCassandraRowForDF(row: CassandraRow): DFCassandraRowWrapper = new DFCassandraRowWrapper(row: CassandraRow)

   val conf = new SparkConf(true).setAppName("Spark Cassandra Demo").set("spark.cassandra.connection.host", "sandbox")
   val sc = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)


   def run(sc: SparkContext, sqlContext: SQLContext) {
      import sqlContext.implicits._

      val watermark = new Timestamp(new DateTime().minusDays(1).getMillis)

      //read events table
      sc.cassandraTable("demo", "event")
      .map(_.toEvent)
      .toDF()
      .registerTempTable("event")

      //read campaign table
      sc.cassandraTable("demo", "campaign")
      .map(_.toCampaign)
      .toDF()
      .registerTempTable("campaign")

      //aggregate events after specific date by (campaign id, type)
      sqlContext.sql { s"""
            SELECT campaignId, eventType, sum(value) as value
            FROM event
            WHERE time > CAST('$watermark' as TIMESTAMP)
            GROUP BY campaignId, eventType
            """.stripMargin
      }.registerTempTable("agg_events")

      sqlContext.sql { s"""
            SELECT id, eventType, sum(value) as total
            FROM campaign
            WHERE day < CAST('$watermark' as TIMESTAMP)
            GROUP BY id, eventType
            """.stripMargin
      }.registerTempTable("agg_campaigns")

      //joining raw events with rolled-up and summing values
      sqlContext.sql {
         s"""SELECT
            agg_events.campaignId,
            agg_events.eventType,
            agg_events.value + agg_campaigns.total as events
            FROM agg_events
            JOIN agg_campaigns
            ON agg_events.campaignId = agg_campaigns.id AND agg_events.eventType = agg_campaigns.eventType
            """.stripMargin
      }.registerTempTable("joined")

      sqlContext.sql {
         s"""SELECT campaignId, sum(events) as total
             FROM joined
             GROUP BY campaignId
            """.stripMargin
      }.registerTempTable("totals")

      val totalsWithBreakdown =  sqlContext.sql {
         s"""
             SELECT joined.campaignId, total, eventType, events
             FROM joined
             JOIN totals
             ON joined.campaignId = totals.campaignId
             ORDER BY total DESC
          """.stripMargin
      }.take(15)

      //top 10 aggregated raw events
      val eventTotals =
         sqlContext.sql("SELECT * FROM agg_events ORDER BY value DESC LIMIT 10")
         .collect()

      //top 10 campaigns by total amount of events
      val campaignTotals =
         sqlContext.sql("SELECT id, sum(total) as result FROM agg_campaigns GROUP BY id ORDER BY result DESC LIMIT 10")
         .collect()

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