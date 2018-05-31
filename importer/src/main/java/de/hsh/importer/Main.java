package de.hsh.importer;

import de.hsh.importer.data.Slice;
import org.voltdb.types.TimestampType;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        int workersCount = 16;
        String import_file = "/tmp/dl/outside.tcpdump.json";


        System.out.println("Indexing datafile..");

        DataSlicer slicer = new DataSlicer(import_file);
        Slice[] slices = slicer.getSlicesByAvgLength(5000);
        ArrayList<Slice[]> workerSlices = Misc.splitSlices(slices, workersCount);

        ArrayList<ImportWorker> workers = new ArrayList<ImportWorker>();
	    for(int i=0; i<workersCount; i++) {
            ImportWorker worker = new ImportWorker(import_file, workerSlices.get(i));
            worker.start();
            workers.add(worker);
        }


        long prev_insert = 0;
	    while(true) {
	        long current_inserts = 0;

            System.out.print("\033[H\033[2J");
            System.out.flush();

            System.out.println("Worker Status:");
            System.out.println("-------------------------");

            for(int i=0; i<workers.size(); i++) {
                System.out.println("Worker #"+i+": "+workers.get(i).getProcessedItems());
                current_inserts += workers.get(i).getProcessedItems();
            }

            System.out.println("-------------------------");
            System.out.println((current_inserts-prev_insert)+" Inserts/sec");
            prev_insert = current_inserts;

            double percent = (double)current_inserts/slicer.getItemCount();
            percent = percent*100;
            System.out.println(String.format("%.2f", percent)+"% handled");

            TimeUnit.SECONDS.sleep(1);
        }
    }
}
