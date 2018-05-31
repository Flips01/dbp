package de.hsh.importer.worker;

import de.hsh.importer.DataReader;
import de.hsh.importer.helper.Misc;
import de.hsh.importer.data.Package;
import de.hsh.importer.data.Server;
import de.hsh.importer.data.Slice;
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
    private Client dbClient;
    private boolean stopped;
    private long processedItems;

    private QueryCounter qCounter;

    public ImportWorker(String import_file, Slice[] slices) {
        this.reader = new DataReader(import_file, slices);
        this.reader.start();
        this.stopped = false;
        this.processedItems = 0;
        this.qCounter = new QueryCounter();
    }

    private void initVoltdb() {
        Server[] servers = Misc.getServersRandomOrder();

        Client client = null;
        ClientConfig config = null;

        config = new ClientConfig("","");
        config.setTopologyChangeAware(true);
        client = ClientFactory.createClient(config);
        try {
            for(Server server : servers) {
                client.createConnection(server.getIp(), server.getPort());
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        this.dbClient = client;
    }

    private void destroyVoltdb() {
        try {
            this.dbClient.close();
        } catch (InterruptedException e) {}
    }

    public void handlePackage(Package p) {
        this.processedItems += 1;

        try {
            TimestampType tst = new TimestampType(p.msFromEpoch());

            String connectionID = UUID.randomUUID().toString();
            int partitionKey = Misc.partitionTs(p.getTs(), 1800);

            this.dbClient.callProcedure(new CounterCallback(this.qCounter), "CONNECTIONS.insert",
                partitionKey,
                connectionID,
                new TimestampType(new Date(p.msFromEpoch())),
                p.getSrcIP(),
                p.getDstIP(),
                p.getSrcPort(),
                p.getDstPort(),
                p.getFlag()
            );

            this.dbClient.callProcedure(new CounterCallback(this.qCounter),"PAYLOAD.insert",
                    partitionKey,
                    connectionID,
                    p.getPayload()
            );

            this.dbClient.callProcedure(new CounterCallback(this.qCounter),"PORT_CONNECTIONS.insert",
                    p.getDstPort(),
                    p.getSrcIP()
            );

            if(WKPQuery.getInstance().isWKP(p.getDstPort())) {
                this.dbClient.callProcedure(new CounterCallback(this.qCounter), "WELL_KNOWN_PORTS.insert",
                        p.getDstIP(),
                        p.getDstPort()
                );
            }

            this.dbClient.callProcedure(new CounterCallback(this.qCounter),"UpsertAverageDataVolume",
                p.getSrcIP(),
                p.getDstIP(),
                    (p.getPayload().length()/2)
            );
        }catch (Exception e) {
            //e.printStackTrace();
            //System.exit(-1);
        }
    }

    public void run(){
        this.initVoltdb();

        while(!this.stopped || this.reader.isFinished()) {
            Package[] batch = this.reader.getBatch();
            for(Package p : batch) {
                this.handlePackage(p);
            }
        }

        this.destroyVoltdb();
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
