package com.job4me.ingest;

/** Same shape as the consumer-side copies in chord-detection and sink-service. */
public record NoteEvent(String sourceId, String type, int note, int velocity, long timestamp) {}
