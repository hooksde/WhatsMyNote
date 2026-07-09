package com.job4me.midicapture

import android.media.midi.MidiReceiver

/**
 * Translates a raw USB MIDI byte stream into NoteEvents, same logic as the
 * desktop app's NoteReceiver (javax.sound.midi.ShortMessage parsing) but
 * against Android's raw MidiReceiver.onSend buffer instead.
 *
 * A single onSend call can contain more than one message (e.g. a fast chord,
 * or System Real-Time bytes like MIDI clock interleaved between them), so
 * this walks the buffer rather than assuming one message per call.
 */
class MidiNoteReceiver(
    private val sourceId: String,
    private val onNote: (NoteEvent) -> Unit
) : MidiReceiver() {

    override fun onSend(msg: ByteArray, offset: Int, count: Int, timestamp: Long) {
        var i = offset
        val end = offset + count
        while (i < end) {
            val status = msg[i].toInt() and 0xFF

            if (status < 0x80) {
                i += 1 // stray data byte with no status; skip
                continue
            }
            if (status >= 0xF8) {
                i += 1 // System Real-Time (clock/start/stop/etc.), always 1 byte
                continue
            }

            val command = status and 0xF0
            when (command) {
                0x90, 0x80 -> {
                    if (i + 2 >= end) break // incomplete message at the end of this buffer
                    val note = msg[i + 1].toInt() and 0x7F
                    val velocity = msg[i + 2].toInt() and 0x7F
                    // many keyboards send "released" as NOTE_ON with velocity 0 (running status)
                    val type = if (command == 0x90 && velocity > 0) "NOTE_ON" else "NOTE_OFF"
                    onNote(NoteEvent(sourceId, type, note, velocity, System.currentTimeMillis()))
                    i += 3
                }
                0xC0, 0xD0 -> i += 2 // Program Change / Channel Pressure -- not a note, 2 bytes
                else -> i += 3 // CC, Pitch Bend, Poly Aftertouch -- not a note, 3 bytes
            }
        }
    }
}
