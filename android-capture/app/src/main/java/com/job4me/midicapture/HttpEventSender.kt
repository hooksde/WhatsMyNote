package com.job4me.midicapture

import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Fire-and-forget POST so the MIDI receiver callback (called on a binder
 * thread, not the UI thread, but still not one we want to block) never waits
 * on the network -- same reasoning as the desktop MidiCaptureApp's
 * HttpEventSender.
 */
class HttpEventSender(
    private val ingestUrlProvider: () -> String,
    private val onResult: (NoteEvent, Boolean, String) -> Unit
) {
    private val executor: ExecutorService = Executors.newSingleThreadExecutor()

    fun send(event: NoteEvent) {
        executor.execute {
            val url = ingestUrlProvider()
            try {
                val conn = URL(url).openConnection() as HttpURLConnection
                conn.requestMethod = "POST"
                conn.setRequestProperty("Content-Type", "application/json")
                conn.doOutput = true
                conn.connectTimeout = 2000
                conn.readTimeout = 2000
                OutputStreamWriter(conn.outputStream).use { it.write(event.toJson()) }
                val code = conn.responseCode
                conn.disconnect()
                onResult(event, code in 200..299, "HTTP $code")
            } catch (e: Exception) {
                onResult(event, false, e.message ?: e.javaClass.simpleName)
            }
        }
    }

    fun shutdown() {
        executor.shutdown()
    }
}
