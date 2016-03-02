package io.datastrophic.spark.workshop.domain

import java.sql.Timestamp

import com.datastax.spark.connector.CassandraRow

/**
 * DataFrame API has limited support for Java/Scala types, so some conversions are needed
 * to properly address this limitation.
 *
 * e.g. replace UUID with String and DateTime with java.sql.Timestamp
 */

case class DFEvent(id: String, campaignId: String, eventType: String, value: Long, time: Timestamp)
case class DFCampaign(id: String, eventType: String, day: Timestamp, value: Long)

class DFCassandraRowWrapper(row: CassandraRow) {
  def toEvent = {
    DFEvent(
      id = row.getUUID("id").toString,
      campaignId = row.getUUID("campaign_id").toString,
      eventType = row.getString("event_type"),
      value = row.getLong("value"),
      time = new Timestamp(row.getDateTime("time").getMillis)
    )
  }

  def toCampaign = {
    DFCampaign(
      id = row.getUUID("id").toString,
      eventType = row.getString("event_type"),
      day = new Timestamp(row.getDateTime("day").getMillis),
      value = row.getLong("value")
    )
  }
}
