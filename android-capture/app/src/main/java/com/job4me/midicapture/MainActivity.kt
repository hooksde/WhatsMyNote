package com.job4me.midicapture

import android.content.Context
import android.media.midi.MidiDevice
import android.media.midi.MidiDeviceInfo
import android.media.midi.MidiManager
import android.media.midi.MidiOutputPort
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.widget.ArrayAdapter
import androidx.appcompat.app.AppCompatActivity
import com.job4me.midicapture.databinding.ActivityMainBinding

/**
 * Android edge producer -- the same role as the desktop MidiCaptureApp
 * (midi-capture/), just running on the phone plugged into a USB MIDI
 * keyboard instead of a machine's onboard MIDI port. Talks to the same
 * ingest gateway over HTTP with the same NoteEvent JSON shape; no Kafka
 * client here either, same reasoning as the desktop app (edge stays thin).
 */
class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private lateinit var midiManager: MidiManager
    private val mainHandler = Handler(Looper.getMainLooper())

    private var devices: List<MidiDeviceInfo> = emptyList()
    private var openDevice: MidiDevice? = null
    private var outputPort: MidiOutputPort? = null
    private var receiver: MidiNoteReceiver? = null
    private lateinit var sender: HttpEventSender

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        if (!packageManager.hasSystemFeature(android.content.pm.PackageManager.FEATURE_MIDI)) {
            setStatus("This device does not support the Android MIDI API.")
            binding.connectButton.isEnabled = false
            binding.refreshDevicesButton.isEnabled = false
            return
        }
        midiManager = getSystemService(Context.MIDI_SERVICE) as MidiManager

        sender = HttpEventSender(
            ingestUrlProvider = { binding.ingestUrlInput.text.toString().trim() },
            onResult = { event, ok, detail -> mainHandler.post { logSend(event, ok, detail) } }
        )

        binding.refreshDevicesButton.setOnClickListener { refreshDevices() }
        binding.connectButton.setOnClickListener {
            if (openDevice == null) connectSelectedDevice() else disconnect()
        }

        refreshDevices()
        setStatus("Disconnected")
    }

    private fun refreshDevices() {
        // TRANSPORT_MIDI_BYTE_STREAM = legacy raw-byte-stream USB MIDI, matching
        // MidiNoteReceiver's parsing (as opposed to Universal MIDI Packets/MIDI 2.0).
        devices = midiManager.getDevicesForTransport(MidiManager.TRANSPORT_MIDI_BYTE_STREAM).toList()
        val names = if (devices.isEmpty()) {
            listOf(getString(R.string.no_devices))
        } else {
            devices.map { it.deviceName() }
        }
        binding.deviceSpinner.adapter =
            ArrayAdapter(this, android.R.layout.simple_spinner_dropdown_item, names)
    }

    private fun connectSelectedDevice() {
        val index = binding.deviceSpinner.selectedItemPosition
        val info = devices.getOrNull(index) ?: return
        setStatus("Opening ${info.deviceName()}...")

        midiManager.openDevice(info, { device ->
            mainHandler.post { onDeviceOpened(device, info) }
        }, mainHandler)
    }

    private fun onDeviceOpened(device: MidiDevice?, info: MidiDeviceInfo) {
        if (device == null) {
            setStatus("Failed to open ${info.deviceName()} (permission denied or device gone)")
            return
        }
        if (info.outputPortCount == 0) {
            setStatus("${info.deviceName()} has no output port to read notes from")
            device.close()
            return
        }

        // A USB MIDI keyboard's "output port" is where IT sends data OUT, i.e.
        // our input. Most simple keyboards expose exactly one; port 0 covers
        // the common case for a demo. Multi-port interfaces would need a
        // picker here instead.
        val port = device.openOutputPort(0)
        if (port == null) {
            setStatus("Could not open output port 0 on ${info.deviceName()}")
            device.close()
            return
        }

        val noteReceiver = MidiNoteReceiver(binding.sourceIdInput.text.toString().trim()) { event ->
            sender.send(event)
            mainHandler.post { appendLog("${event.type} note=${event.note} vel=${event.velocity}") }
        }
        port.connect(noteReceiver)

        openDevice = device
        outputPort = port
        receiver = noteReceiver
        setStatus("Connected: ${info.deviceName()}")
        binding.connectButton.text = getString(R.string.disconnect)
    }

    private fun disconnect() {
        receiver?.let { outputPort?.disconnect(it) }
        outputPort?.close()
        openDevice?.close()
        outputPort = null
        openDevice = null
        receiver = null
        setStatus("Disconnected")
        binding.connectButton.text = getString(R.string.connect)
    }

    private fun logSend(event: NoteEvent, ok: Boolean, detail: String) {
        val marker = if (ok) "OK" else "FAIL"
        appendLog("  -> POST $marker ($detail)")
    }

    private fun setStatus(text: String) {
        binding.statusText.text = text
    }

    private fun appendLog(line: String) {
        val current = binding.logText.text
        val next = if (current.isNullOrEmpty()) line else "$current\n$line"
        // keep the log from growing without bound over a long demo session
        binding.logText.text = next.lines().takeLast(200).joinToString("\n")
    }

    override fun onDestroy() {
        disconnect()
        sender.shutdown()
        super.onDestroy()
    }

    private fun MidiDeviceInfo.deviceName(): String =
        properties.getString(MidiDeviceInfo.PROPERTY_NAME)
            ?: properties.getString(MidiDeviceInfo.PROPERTY_PRODUCT)
            ?: "MIDI device $id"
}
