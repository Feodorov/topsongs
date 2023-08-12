package com.kfedorov.topsongs.businesslogic.utils

import com.kfedorov.topsongs.model.TrackLogEntry
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

private[businesslogic] object Sanitizer {

  /** @return input dataset without tracks that miss `trackName`
    */
  def sanitize(dataset: Dataset[TrackLogEntry]): Dataset[TrackLogEntry] =
    dataset.filter(col("trackName").isNotNull)
}
