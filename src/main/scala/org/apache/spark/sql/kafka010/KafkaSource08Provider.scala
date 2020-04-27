package org.apache.spark.sql.kafka010

import java.util.{Locale, UUID}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.kafka010.KafkaSourceProvider.STARTING_OFFSETS_OPTION_KEY
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


class KafkaSource08Provider extends StreamSourceProvider
  with DataSourceRegister with Logging {

  import KafkaSource08Provider._

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    ("kafka08", KafkaSource08.kafkaSchema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {

    val uniqueGroupId = s"spark-kafka-source-${UUID.randomUUID}-${metadataPath.hashCode}"

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase(Locale.ROOT).startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val topics =
      parameters.get(TOPICS) match {
        case Some(s) => s.split(",").map(_.trim).filter(_.nonEmpty).toSet
        case None => throw new IllegalArgumentException(s"$TOPICS should be set.")
      }

    val startingStreamOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(caseInsensitiveParams,
      STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)

    var kafkaParams = KafkaSourceProvider
      .kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId)
      .asScala
      .filter({ case (k, _) => ConsumerConfig.AUTO_OFFSET_RESET_CONFIG != k })
      .map({ case (k, v) => (k, String.valueOf(v)) })
      .toMap[String, String]


    kafkaParams += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest")

    new KafkaSource08(
      sqlContext,
      topics,
      kafkaParams,
      parameters,
      metadataPath,
      startingStreamOffsets
    )
  }

  override def shortName(): String = "kafka08"

}

private[kafka010] object KafkaSource08Provider {
  private val TOPICS = "subscribe"
}