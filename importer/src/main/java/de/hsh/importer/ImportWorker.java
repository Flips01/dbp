package de.hsh.importer;

import de.hsh.importer.data.Package;
import de.hsh.importer.data.Server;
import de.hsh.importer.data.Slice;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.types.TimestampType;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class ImportWorker extends Thread {
    private DataReader reader;
    private Client dbClient;
    private boolean stopped;
    private long processedItems;
    private Random test;

    public ImportWorker(String import_file, Slice[] slices) {
        this.reader = new DataReader(import_file, slices);
        this.reader.start();
        this.stopped = false;
        this.processedItems = 0;
        this.test = new Random();
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
            this.dbClient.callProcedure("CONNECTIONS.insert",
                    this.test.nextInt(),
                    UUID.randomUUID().toString(),
                    new TimestampType(p.msFromEpoch()),
                    p.getSrcIP(),
                    p.getDstIP(),
                    p.getSrcPort(),
                    p.getDstPort(),
                    p.getFlag()
                    );

            /*
            this.dbClient.callProcedure("InsertActiveConnectionsAndPayload",
                    new TimestampType(p.msFromEpoch()),
                    p.getSrcIP(),
                    p.getDstIP(),
                    p.getSrcPort(),
                    p.getDstPort(),
                    p.getFlag(),
                    p.getPayload()
                    );
            /*
            this.dbClient.callProcedure("InsertPacket",
                    new TimestampType(p.msFromEpoch()),
                    p.getSrcIP(),
                    p.getDstIP(),
                    p.getSrcPort(),
                    p.getDstPort(),
                    p.getFlag(),
                    p.getPayload(),
                    p.getType(),
                    p.getSize()
            );
            */
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
}
