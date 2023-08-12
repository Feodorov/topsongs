package com.kfedorov.topsongs.model

import java.sql.Timestamp

case class TrackLogEntry(
    userId: String,
    timestamp: Timestamp,
    artistId: String,
    artistName: String,
    trackId: String,
    trackName: String
)
