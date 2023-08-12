package com.kfedorov.topsongs.businesslogic.utils

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.kfedorov.topsongs.businesslogic.SparkSessionHelper
import com.kfedorov.topsongs.model.{Track, TrackLogEntryWithSession}
import org.scalatest.funspec.AnyFunSpec

class SessionRankingTest
    extends AnyFunSpec
    with SparkSessionHelper
    with DatasetComparer {
  describe("session ranking") {
    import spark.implicits._
    val track = TrackLogEntryWithSession(
      artistName = "artist 1 name",
      trackName = "track 1 name",
      sessionId = "session 1"
    )

    it("should select the longest session") {
      val dataset = spark.createDataset(
        Seq(
          // two tracks from session 1
          track,
          track.copy(trackName = "track 2 name"),
          // one track from session 2
          track.copy(sessionId = "session 2")
        )
      )

      val actual = SessionRanking.tracksFromTopSessions(dataset, 1)

      val expected =
        spark.createDataset(
          Seq(
            Track(
              artistName = "artist 1 name",
              trackName = "track 1 name"
            ),
            Track(
              artistName = "artist 1 name",
              trackName = "track 2 name"
            )
          )
        )
      assertSmallDatasetEquality(actual, expected)
    }
  }
}
