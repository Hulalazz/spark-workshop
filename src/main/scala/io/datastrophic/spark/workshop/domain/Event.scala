package io.datastrophic.spark.workshop.domain

import java.util.UUID

import com.datastax.spark.connector.CassandraRow
import org.joda.time.DateTime

case class Event(id: UUID, campaignId: UUID, eventType: String, value: Long, time: DateTime) {
  def +(other: Event) = copy(value = value + other.value)
}

case class Campaign(id: UUID, eventType: String, day: DateTime, value: Long){
  def +(other: Campaign) = copy(value = value + other.value)
}

case class CampaignTotals(id: UUID, ad_type: String, value: Long){
  def +(other: CampaignTotals) = copy(value = value + other.value)
}
object CampaignTotals{
  def apply(campaign: Campaign, event: Event) = new CampaignTotals(campaign.id, campaign.eventType, campaign.value + event.value)
}

class CassandraRowWrapper(row: CassandraRow) {
  def toEvent = {
    Event(
      id = row.getUUID("id"),
      campaignId = row.getUUID("campaign_id"),
      eventType = row.getString("event_type"),
      value = row.getLong("value"),
      time = row.getDateTime("time")
    )
  }

  def toCampaign = {
    Campaign(
      id = row.getUUID("id"),
      eventType = row.getString("event_type"),
      day = row.getDateTime("day"),
      value = row.getLong("value")
    )
  }

  def toCampaignTotals = {
    CampaignTotals(
      id = row.getUUID("id"),
      ad_type = row.getString("event_type"),
      value = row.getLong("value")
    )
  }
}
