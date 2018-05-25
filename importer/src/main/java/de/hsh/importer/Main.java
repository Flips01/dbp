package de.hsh.importer;

import org.voltdb.types.TimestampType;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        DataReader dr = new DataReader("/tmp/outside.tcpdump.json");
	    dr.start();

	    int workersCount = 8;
        ArrayList<ImportWorker> workers = new ArrayList<ImportWorker>();
	    for(int i=0; i<workersCount; i++) {
            ImportWorker worker = new ImportWorker("192.168.58.2", 21212, dr);
            worker.start();
            workers.add(worker);
        }


	    while(true) {
            System.out.print("\033[H\033[2J");
            System.out.flush();

            System.out.println("Worker Status:");
            System.out.println("-------------------------");

            for(int i=0; i<workers.size(); i++) {
                System.out.println("Worker #"+i+": "+workers.get(i).getProcessedItems());
            }

            System.out.println("-------------------------");
            System.out.println("Queued Items: "+dr.queuedItems());

            TimeUnit.SECONDS.sleep(1);
        }
    }
}
