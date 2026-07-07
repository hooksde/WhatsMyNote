package com.example.midi;

import javax.sound.midi.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * Standalone edge producer: captures MIDI from a physical keyboard and ships
 * each note on/off as an event to the cloud ingest service.
 *
 * Runs on the machine the keyboard is plugged into. No Spring, no Kafka client
 * here on purpose -- the edge stays thin and talks to an ingest gateway over HTTP.
 *
 * Build/run (Java 17+):
 *   javac MidiCaptureApp.java
 *   java com.example.midi.MidiCaptureApp                  # uses first input device
 *   java com.example.midi.MidiCaptureApp "Behringer"      # match device by name
 */
public class MidiCaptureApp {

    private static final String SOURCE_ID  = "studio-keyboard-1";
    private static final String INGEST_URL = "http://localhost:8080/api/notes";

    public static void main(String[] args) throws Exception {
        String nameFilter = args.length > 0 ? args[0] : null;

        listDevices();                                  // so you can see what's available
        MidiDevice input = openInputDevice(nameFilter);
        System.out.println("Listening on: " + input.getDeviceInfo().getName());

        EventSender sender = new HttpEventSender(INGEST_URL);
        input.getTransmitter().setReceiver(new NoteReceiver(SOURCE_ID, sender));

        // MIDI callbacks fire on a background thread, so keep main alive.
        Runtime.getRuntime().addShutdownHook(new Thread(input::close));
        new CountDownLatch(1).await();
    }

    /** Opens the first transmitter-capable device, optionally filtered by name. */
    private static MidiDevice openInputDevice(String nameFilter) throws MidiUnavailableException {
        for (MidiDevice.Info info : MidiSystem.getMidiDeviceInfo()) {
            MidiDevice device = MidiSystem.getMidiDevice(info);
            // maxTransmitters != 0  ->  the device can SEND MIDI to us (a keyboard input).
            boolean canTransmit = device.getMaxTransmitters() != 0;
            boolean isRealInput = !(device instanceof Sequencer) && !(device instanceof Synthesizer);
            boolean nameOk = nameFilter == null || info.getName().toLowerCase().contains(nameFilter.toLowerCase());
            if (canTransmit && isRealInput && nameOk) {
                device.open();
                return device;
            }
        }
        throw new IllegalStateException("No matching MIDI input found. Is the keyboard plugged in?");
    }

    private static void listDevices() throws MidiUnavailableException {
        System.out.println("Available MIDI devices:");
        for (MidiDevice.Info info : MidiSystem.getMidiDeviceInfo()) {
            MidiDevice d = MidiSystem.getMidiDevice(info);
            String dir = d.getMaxTransmitters() != 0 ? "IN " : "OUT";
            System.out.printf("  [%s] %s%n", dir, info.getName());
        }
    }

    /** The note shape we publish. In a real system, share this contract via a schema. */
    public record NoteEvent(String sourceId, String type, int note, int velocity, long timestamp) {}

    /** Translates raw MIDI ShortMessages into NoteEvents. */
    static final class NoteReceiver implements Receiver {
        private final String sourceId;
        private final EventSender sender;

        NoteReceiver(String sourceId, EventSender sender) {
            this.sourceId = sourceId;
            this.sender = sender;
        }

        @Override
        public void send(MidiMessage message, long timeStamp) {
            if (!(message instanceof ShortMessage sm)) return;   // ignore SysEx / meta

            int command  = sm.getCommand();   // channel nibble already stripped
            int note     = sm.getData1();      // 0-127, middle C = 60
            int velocity = sm.getData2();      // 0-127

            String type;
            if (command == ShortMessage.NOTE_ON && velocity > 0) {
                type = "NOTE_ON";
            } else if (command == ShortMessage.NOTE_OFF
                    || (command == ShortMessage.NOTE_ON && velocity == 0)) {
                // many keyboards send "released" as NOTE_ON with velocity 0
                type = "NOTE_OFF";
            } else {
                return;   // control change, pitch bend, aftertouch, etc.
            }

            sender.send(new NoteEvent(sourceId, type, note, velocity, System.currentTimeMillis()));
        }

        @Override public void close() { }
    }

    interface EventSender {
        void send(NoteEvent event);
    }

    /** Fire-and-forget async POST so the MIDI thread never blocks on the network. */
    static final class HttpEventSender implements EventSender {
        private final HttpClient client = HttpClient.newHttpClient();
        private final String url;

        HttpEventSender(String url) { this.url = url; }

        @Override
        public void send(NoteEvent e) {
            String json = """
                {"sourceId":"%s","type":"%s","note":%d,"velocity":%d,"timestamp":%d}"""
                .formatted(e.sourceId(), e.type(), e.note(), e.velocity(), e.timestamp());

            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(2))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            client.sendAsync(req, HttpResponse.BodyHandlers.discarding())
                  .exceptionally(ex -> { System.err.println("send failed: " + ex.getMessage()); return null; });
        }
    }
}
