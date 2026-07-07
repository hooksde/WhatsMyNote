package com.example.sink;

/** Same contract as the producer side. In production, share this via a schema/registry. */
public record NoteEvent(String sourceId, String type, int note, int velocity, long timestamp) {}
