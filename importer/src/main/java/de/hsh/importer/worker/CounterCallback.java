package de.hsh.importer.worker;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

class CounterCallback implements ProcedureCallback {
    private QueryCounter qCounter;

    public CounterCallback(QueryCounter qCounter) {
        this.qCounter = qCounter;
        this.qCounter.queryIssued();
    }

    public void clientCallback(ClientResponse response) {

        /*
        // Make sure the procedure succeeded.
        if (response.getStatus() != ClientResponse.SUCCESS) {
            System.err.println(response.getStatusString());
            return;
        }

        VoltTable results[] = response.getResults();
        VoltTable recordset = results[0];

        System.out.printf("%s, %s!\n",
                recordset.fetchRow(0).getString("Hello"),
                recordset.fetchRow(0).getString("Firstname") );
                */

        this.qCounter.queryCompleted();
    }
}