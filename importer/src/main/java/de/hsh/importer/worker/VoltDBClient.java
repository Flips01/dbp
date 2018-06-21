package de.hsh.importer.worker;

import de.hsh.importer.data.Server;
import de.hsh.importer.helper.Misc;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.types.TimestampType;

import java.io.IOException;

public class VoltDBClient {
    private static VoltDBClient instance;

    public static synchronized VoltDBClient getInstance () {
        if (VoltDBClient.instance == null) {
            VoltDBClient.instance = new VoltDBClient();
        }
        return VoltDBClient.instance;
    }

    public static synchronized void destroy() {
        if (VoltDBClient.instance == null) {
            return;
        }

        VoltDBClient.instance.close();
        VoltDBClient.instance = null;
    }

    ///////////////////////////////////////////////////////////

    private Client dbClient;
    private VoltDBClient () {
        Server[] servers = Misc.getServersRandomOrder();

        Client client = null;
        ClientConfig config = null;

        config = new ClientConfig("","");
        config.setTopologyChangeAware(true);
        config.setHeavyweight(true);
        config.setMaxOutstandingTxns(100000);

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

    public Client get() {
        return this.dbClient;
    }

    private void close() {
        try {
            this.dbClient.close();
        } catch (Exception ex) {}
    }

}
