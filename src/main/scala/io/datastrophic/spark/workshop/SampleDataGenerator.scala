package io.datastrophic.spark.workshop

import java.util.UUID

import io.datastrophic.spark.workshop.domain.{Campaign, Event}
import org.joda.time.DateTime

import scala.util.Random

object SampleDataGenerator {
   def createEvent(id: UUID, timestamp: DateTime): Event =
      Event(
         id = UUID.randomUUID(),
         campaignId = id,
         eventType = randomEventType(),
         value = Random.nextInt(5),
         time = timestamp
      )

   def createCampaign(id: UUID, timestamp: DateTime): Campaign =
      Campaign(
         id = id,
         eventType = randomEventType(),
         day = timestamp,
         value = Random.nextInt(1000)
      )

   val eventTypes = List(
      "impression",
      "click",
      "close"
   )

   def randomEventType(): String = eventTypes(Random.nextInt(eventTypes.size))

   def generateCampaigns(amount: Int, spanDays: Int): List[Campaign] = {
      (1 to amount) flatMap { c => 
         val id = UUID.randomUUID()
         (1 to spanDays) map { d => createCampaign(id, new DateTime().minusDays(c)) }
      } toList
   }

   def generateEventsForCampaign(campaignId: UUID, amount: Int): List[Event] = {
      (1 to amount) map { c => createEvent(campaignId, new DateTime().minusMinutes(c))} toList
   }
}