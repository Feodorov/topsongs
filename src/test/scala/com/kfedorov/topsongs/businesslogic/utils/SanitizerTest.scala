package com.kfedorov.topsongs.businesslogic.utils

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.kfedorov.topsongs.businesslogic.SparkSessionHelper
import com.kfedorov.topsongs.model.TrackLogEntry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.sql.Timestamp
import java.time.Instant

class SanitizerTest
    extends AnyFunSpec
    with SparkSessionHelper
    with DatasetComparer {
  describe("sanitizer") {

    it("should remove tracks with missing track names") {
      import spark.implicits._
      val trackWithName = TrackLogEntry(
        userId = "user 1",
        timestamp = Timestamp.from(Instant.now()),
        artistId = "artist 1 id",
        artistName = "artist 1 name",
        trackId = "track 1 id",
        trackName = "track 1 name"
      )
      val trackWithoutName = trackWithName.copy(trackName = null)
      val dataset = spark.createDataset(Seq(trackWithName, trackWithoutName))

      val actual = Sanitizer.sanitize(dataset)

      val expected = spark.createDataset(Seq(trackWithName))
      assertSmallDatasetEquality(actual, expected)
    }
  }
}
