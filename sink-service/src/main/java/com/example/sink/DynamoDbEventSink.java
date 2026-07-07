package com.example.sink;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

/**
 * DynamoDB sink. Uses the standard AWS credential/region chain
 * (env vars, profile, or instance role) -- no keys in code.
 *
 * Suggested tables:
 *   note-events : PK sourceId (S), SK timestamp (N)
 *   chord-events: PK sourceId (S), SK startTs (N)
 */
@Component
@Profile("dynamodb")
public class DynamoDbEventSink implements EventSink {

    private final DynamoDbClient db = DynamoDbClient.create();

    @Value("${sink.dynamodb.note-table:note-events}")
    private String noteTable;
    @Value("${sink.dynamodb.chord-table:chord-events}")
    private String chordTable;

    @Override
    public void write(NoteEvent n) {
        db.putItem(PutItemRequest.builder()
                .tableName(noteTable)
                .item(Map.of(
                        "sourceId",  AttributeValue.fromS(n.sourceId()),
                        "timestamp", AttributeValue.fromN(Long.toString(n.timestamp())),
                        "type",      AttributeValue.fromS(n.type()),
                        "note",      AttributeValue.fromN(Integer.toString(n.note())),
                        "velocity",  AttributeValue.fromN(Integer.toString(n.velocity()))))
                .build());
    }

    @Override
    public void write(ChordEvent c) {
        db.putItem(PutItemRequest.builder()
                .tableName(chordTable)
                .item(Map.of(
                        "sourceId", AttributeValue.fromS(c.sourceId()),
                        "startTs",  AttributeValue.fromN(Long.toString(c.startTs())),
                        "endTs",    AttributeValue.fromN(Long.toString(c.endTs())),
                        "notes",    AttributeValue.fromL(c.notes().stream()
                                .map(x -> AttributeValue.fromN(Integer.toString(x)))
                                .toList())))
                .build());
    }
}
