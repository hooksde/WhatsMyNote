package com.job4me.midicapture

/** Same wire shape as com.job4me.ingest.NoteEvent -- keep fields in sync. */
data class NoteEvent(
    val sourceId: String,
    val type: String,
    val note: Int,
    val velocity: Int,
    val timestamp: Long
) {
    fun toJson(): String =
        """{"sourceId":"$sourceId","type":"$type","note":$note,"velocity":$velocity,"timestamp":$timestamp}"""
}
