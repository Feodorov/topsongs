package com.kfedorov.topsongs.businesslogic

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.kfedorov.topsongs.model.{
  Track,
  TrackLogEntry,
  TrackLogEntryWithSession
}
import org.scalatest.funspec.AnyFunSpec

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

class TopSongsTest
    extends AnyFunSpec
    with SparkSessionHelper
    with DatasetComparer {

  import spark.implicits._

  describe("top songs") {
    val now = Instant.now()
    val track = TrackLogEntry(
      userId = "user_1",
      timestamp = Timestamp.from(now),
      artistId = "artist 1 id",
      artistName = "artist 1 name",
      trackId = "track 1 id",
      trackName = "track 1 name"
    )

    it("should select top song from top session") {
      val dataset = spark.createDataset(
        Seq(
          // user 1 session 1 - the longest with 2 tracks
          track,
          track.copy(timestamp =
            Timestamp.from(now.plus(20, ChronoUnit.MINUTES))
          ),
          // user 1 session 2 - after 20 minutes from session 1
          track.copy(
            trackName = "track 2 name",
            timestamp = Timestamp.from(now.plus(21, ChronoUnit.MINUTES))
          ),
          // another user with 1 track
          track.copy(userId = "user_2")
        )
      )

      val actual = TopSongs.topSongs(dataset, topNSongs = 1, topNSessions = 1)

      val expected = spark.createDataset(
        Seq(
          Track(
            artistName = "artist 1 name",
            trackName = "track 1 name"
          )
        )
      )
      assertSmallDatasetEquality(actual, expected)
    }
  }
}
