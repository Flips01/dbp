package de.hsh.importer.worker;

import de.hsh.importer.DataReader;
import de.hsh.importer.helper.Misc;
import de.hsh.importer.data.Package;
import de.hsh.importer.data.Server;
import de.hsh.importer.data.Slice;
import de.hsh.importer.helper.SingleInsertManager;
import de.hsh.importer.helper.WKPQuery;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.types.TimestampType;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

public class ImportWorker extends Thread {
    private DataReader reader;
    private boolean stopped;
    private long processedItems;

    private VoltDBClient dbClient;
    private QueryCounter qCounter;
    private SingleInsertManager sInsertManager;

    public ImportWorker(String import_file, Slice[] slices, VoltDBClient dbClient) {
        this.reader = new DataReader(import_file, slices);
        this.reader.start();
        this.stopped = false;
        this.processedItems = 0;
        this.qCounter = new QueryCounter();
        this.sInsertManager = new SingleInsertManager();
        this.dbClient = dbClient;
    }

    public void handlePackage(Package p) {
        this.processedItems += 1;

        try {
            TimestampType tst = new TimestampType(p.msFromEpoch());

            String connectionID = UUID.randomUUID().toString();
            int partitionKey = Misc.partitionTs(p.getTs(), 600);

            this.dbClient.get().callProcedure(new CounterCallback(this.qCounter), "CONNECTIONS.insert",
                partitionKey,
                connectionID,
                new TimestampType(new Date(p.msFromEpoch())),
                p.getSrcIP(),
                p.getDstIP(),
                p.getSrcPort(),
                p.getDstPort(),
                p.getFlag()
            );

            this.dbClient.get().callProcedure(new CounterCallback(this.qCounter),"PAYLOAD.insert",
                    partitionKey,
                    connectionID,
                    p.getPayload()
            );

            if(this.sInsertManager.canInsert("PORT_CONNECTIONS.insert", p.getDstPort(), p.getSrcIP())) {
                this.dbClient.get().callProcedure(new CounterCallback(this.qCounter), "PORT_CONNECTIONS.insert",
                        p.getDstPort(),
                        p.getSrcIP()
                );
            }

            if(WKPQuery.getInstance().isWKP(p.getDstPort())) {
                if(this.sInsertManager.canInsert("WELL_KNOWN_PORTS.insert", p.getDstIP(), p.getDstPort())) {
                    this.dbClient.get().callProcedure(new CounterCallback(this.qCounter), "WELL_KNOWN_PORTS.insert",
                            p.getDstIP(),
                            p.getDstPort()
                    );
                }
            }

            this.dbClient.get().callProcedure(new CounterCallback(this.qCounter),"UpsertAverageDataVolume",
                p.getSrcIP(),
                p.getDstIP(),
                p.getPayloadSize()
            );
        }catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void run(){
        while(!this.stopped || this.reader.isFinished()) {
            Package[] batch = this.reader.getBatch();
            for(Package p : batch) {
                this.handlePackage(p);
            }
        }
    }

    public void shutdown() {
        this.stopped = true;
        this.interrupt();
    }

    public long getProcessedItems() {
        return this.processedItems;
    }

    public long getQueueItemsCount(){
        return this.reader.queuedItems();
    }

    public long getCompletedQueries() {
        return this.qCounter.getQueriesCompleted();
    }

    public long getIssuedQueries() {
        return this.qCounter.getQueriesIssued();
    }

    public double getQueryCompletionRate() {
        return this.qCounter.getCompletionRate()*100;
    }

    public boolean isRunning() {
        return this.isAlive() || this.getQueueItemsCount() > 0 || this.getQueryCompletionRate() != 1;
    }
}
