package com.example.chord;

/** Mirrors the events the ingest service produces to the note-events topic. */
public record NoteEvent(String sourceId, String type, int note, int velocity, long timestamp) {}
