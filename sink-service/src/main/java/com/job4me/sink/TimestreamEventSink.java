package com.job4me.sink;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

/**
 * Amazon Timestream sink -- a natural fit for time-stamped note/chord events.
 * Each note is a BIGINT measure; each chord is stored as a VARCHAR like "[60, 64, 67]".
 * Uses the standard AWS credential/region chain.
 */
@Component
@Profile("timestream")
public class TimestreamEventSink implements EventSink {

    private final TimestreamWriteClient ts = TimestreamWriteClient.create();

    @Value("${sink.timestream.database:piano}")
    private String database;
    @Value("${sink.timestream.note-table:note-events}")
    private String noteTable;
    @Value("${sink.timestream.chord-table:chord-events}")
    private String chordTable;

    @Override
    public void write(NoteEvent n) {
        Record record = Record.builder()
                .dimensions(
                        Dimension.builder().name("sourceId").value(n.sourceId()).build(),
                        Dimension.builder().name("type").value(n.type()).build())
                .measureName("note")
                .measureValueType(MeasureValueType.BIGINT)
                .measureValue(Integer.toString(n.note()))
                .time(Long.toString(n.timestamp()))
                .timeUnit(TimeUnit.MILLISECONDS)
                .build();

        ts.writeRecords(WriteRecordsRequest.builder()
                .databaseName(database).tableName(noteTable).records(record).build());
    }

    @Override
    public void write(ChordEvent c) {
        Record record = Record.builder()
                .dimensions(Dimension.builder().name("sourceId").value(c.sourceId()).build())
                .measureName("chord")
                .measureValueType(MeasureValueType.VARCHAR)
                .measureValue(c.notes().toString())
                .time(Long.toString(c.startTs()))
                .timeUnit(TimeUnit.MILLISECONDS)
                .build();

        ts.writeRecords(WriteRecordsRequest.builder()
                .databaseName(database).tableName(chordTable).records(record).build());
    }
}
