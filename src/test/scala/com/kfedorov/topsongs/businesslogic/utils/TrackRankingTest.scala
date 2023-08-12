package com.kfedorov.topsongs.businesslogic.utils

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.kfedorov.topsongs.businesslogic.SparkSessionHelper
import com.kfedorov.topsongs.model.Track
import org.scalatest.funspec.AnyFunSpec

class TrackRankingTest
    extends AnyFunSpec
    with SparkSessionHelper
    with DatasetComparer {
  describe("track ranking") {
    import spark.implicits._
    val track = Track(
      artistName = "artist 1 name",
      trackName = "track 1 name"
    )

    it("should select tracks based on frequency") {
      val dataset = spark.createDataset(
        Seq(track, track, track.copy(trackName = "track 2 name"))
      )

      val actual = TrackRanking.mostPopularTracks(dataset, topNSongs = 1)

      val expected = spark.createDataset(Seq(track))
      assertSmallDatasetEquality(actual, expected)
    }
  }
}
