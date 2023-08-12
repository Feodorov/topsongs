package com.kfedorov.topsongs.businesslogic.utils

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.kfedorov.topsongs.businesslogic.SparkSessionHelper
import com.kfedorov.topsongs.model.{TrackLogEntry, TrackLogEntryWithSession}
import org.scalatest.funspec.AnyFunSpec

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

class SessionMatcherTest
    extends AnyFunSpec
    with SparkSessionHelper
    with DatasetComparer {
  import spark.implicits._

  describe("session matcher") {
    val now = Instant.now()
    val track = TrackLogEntry(
      userId = "user_1",
      timestamp = Timestamp.from(now),
      artistId = "artist 1 id",
      artistName = "artist 1 name",
      trackId = "track 1 id",
      trackName = "track 1 name"
    )

    it(
      "should assign same session id for tracks from the same user within 20 minutes"
    ) {
      val dataset = spark.createDataset(
        Seq(
          track,
          track.copy(timestamp =
            Timestamp.from(now.plus(20, ChronoUnit.MINUTES))
          )
        )
      )

      val actual = SessionMatcher.assignSessionId(dataset)

      // expected same session id for all tracks
      val expected = spark.createDataset(
        Seq(
          TrackLogEntryWithSession(
            artistName = "artist 1 name",
            trackName = "track 1 name",
            sessionId = "user_1_1"
          ),
          TrackLogEntryWithSession(
            artistName = "artist 1 name",
            trackName = "track 1 name",
            sessionId = "user_1_1"
          )
        )
      )
      assertSmallDatasetEquality(actual, expected)
    }

    it(
      "should assign different session id for tracks from the same user with > 20 minutes difference"
    ) {
      val dataset = spark.createDataset(
        Seq(
          track,
          track.copy(timestamp =
            Timestamp.from(
              now.plus(20, ChronoUnit.MINUTES).plus(1, ChronoUnit.SECONDS)
            )
          )
        )
      )

      val actual = SessionMatcher.assignSessionId(dataset)

      // expected different session ids
      val expected = spark.createDataset(
        Seq(
          TrackLogEntryWithSession(
            artistName = "artist 1 name",
            trackName = "track 1 name",
            sessionId = "user_1_1"
          ),
          TrackLogEntryWithSession(
            artistName = "artist 1 name",
            trackName = "track 1 name",
            sessionId = "user_1_2"
          )
        )
      )
      assertSmallDatasetEquality(actual, expected)
    }

    it(
      "should assign different session ids for simultaneous tracks from different users"
    ) {
      val dataset =
        spark.createDataset(Seq(track, track.copy(userId = "user_2")))

      val actual = SessionMatcher.assignSessionId(dataset)

      // expected different session id (one per user).
      val expected = spark.createDataset(
        Seq(
          TrackLogEntryWithSession(
            artistName = "artist 1 name",
            trackName = "track 1 name",
            sessionId = "user_1_1"
          ),
          TrackLogEntryWithSession(
            artistName = "artist 1 name",
            trackName = "track 1 name",
            sessionId = "user_2_1"
          )
        )
      )
      assertSmallDatasetEquality(actual, expected)
    }
  }
}
