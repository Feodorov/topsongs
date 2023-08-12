package com.kfedorov.topsongs.businesslogic.utils

import com.kfedorov.topsongs.model.{TrackLogEntry, TrackLogEntryWithSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

private[businesslogic] object SessionMatcher {

  /** Assigns unique session id to the tracks from the same `userId` that were played
    * within `maxIntervalBetweenTracksSec` between each other. */
  def assignSessionId(
                       dataset: Dataset[TrackLogEntry],
                       maxIntervalBetweenTracksSec: Int = 20 * 60
  ): Dataset[TrackLogEntryWithSession] = {
    import dataset.sparkSession.implicits._

    // Add `prev_timestamp` column.
    // Equals to the `timestamp` of the previous row (in chronological order) for the same user.
    // `null` if it is the first row.
    val withPrevTimestamp = dataset
      .withColumn(
        "prevTimestamp",
        lag(col("timestamp"), 1)
          .over(Window.partitionBy(col("userId")).orderBy(col("timestamp")))
      )

    // Add `isNewSession` column.
    // `true` if it is the first track or if sufficient time (> maxIntervalBetweenTracksSec)
    // passed since previous track.
    val isNewSession = withPrevTimestamp
      .withColumn(
        "isNewSession",
        col("prevTimestamp").isNull
          || unix_timestamp(col("timestamp")) - unix_timestamp(
            col("prevTimestamp")
          ) > maxIntervalBetweenTracksSec
      )

    // Assign sequential session ids within the same `userId` scope.
    // Sequential ids are calculated as "number of "isNewSession = true" seen before within the same `userId`".
    val ds = isNewSession.withColumn(
      "sessionId",
      concat(
        col("userId"),
        lit("_"),
        sum(col("isNewSession").cast("long")).over(
          Window
            .partitionBy(col("userId"))
            .orderBy(col("timestamp"))
            .rangeBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
    )

    ds.select(
      col("artistName"),
      col("trackName"),
      col("sessionId")
    ).as[TrackLogEntryWithSession]
  }
}
