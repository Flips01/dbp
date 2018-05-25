package de.hsh.importer;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.types.TimestampType;

import java.io.IOException;

public class ImportWorker extends Thread {
    private String serverHost;
    private int serverPort;
    private DataReader reader;
    private Client dbClient;
    private boolean stopped;
    private long processedItems;

    public ImportWorker(String serverHost, int serverPort, DataReader reader) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.reader = reader;
        this.stopped = false;
        this.processedItems = 0;
    }

    private void initVoltdb() {
        Client client = null;
        ClientConfig config = null;

        config = new ClientConfig("","");
        config.setTopologyChangeAware(true);
        client = ClientFactory.createClient(config);
        try {
            client.createConnection(this.serverHost, this.serverPort);
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
        }catch (Exception e) {
            //e.printStackTrace();
            //System.exit(-1);
        }
    }

    public void run(){
        this.initVoltdb();

        while(!this.stopped) {
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
