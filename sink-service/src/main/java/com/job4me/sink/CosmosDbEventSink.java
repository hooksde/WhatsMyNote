package com.job4me.sink;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Cosmos DB sink -- Azure's DynamoDB analog. Auth is Azure AD via
 * DefaultAzureCredential (the container's managed identity, granted Cosmos DB
 * Built-in Data Contributor in infra-azure/identity.tf) -- no keys in code.
 *
 * Containers use partition key /sourceId, matching the DynamoDB sink's hash
 * key so both sinks shard identically:
 *   note-events : partition key sourceId
 *   chord-events: partition key sourceId
 */
@Component
@Profile("cosmosdb")
public class CosmosDbEventSink implements EventSink {

    private final CosmosContainer noteContainer;
    private final CosmosContainer chordContainer;

    public CosmosDbEventSink(
            @Value("${sink.cosmos.endpoint}") String endpoint,
            @Value("${sink.cosmos.database:piano}") String database,
            @Value("${sink.cosmos.note-container:note-events}") String noteContainerName,
            @Value("${sink.cosmos.chord-container:chord-events}") String chordContainerName) {
        CosmosClient client = new CosmosClientBuilder()
                .endpoint(endpoint)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();
        this.noteContainer = client.getDatabase(database).getContainer(noteContainerName);
        this.chordContainer = client.getDatabase(database).getContainer(chordContainerName);
    }

    @Override
    public void write(NoteEvent n) {
        noteContainer.createItem(Map.of(
                "id", n.sourceId() + "-" + n.timestamp(),
                "sourceId", n.sourceId(),
                "type", n.type(),
                "note", n.note(),
                "velocity", n.velocity(),
                "timestamp", n.timestamp()));
    }

    @Override
    public void write(ChordEvent c) {
        chordContainer.createItem(Map.of(
                "id", c.sourceId() + "-" + c.startTs(),
                "sourceId", c.sourceId(),
                "notes", c.notes(),
                "startTs", c.startTs(),
                "endTs", c.endTs()));
    }
}
