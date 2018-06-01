package de.hsh.importer;

import de.hsh.importer.data.Slice;
import de.hsh.importer.helper.Misc;
import de.hsh.importer.worker.ImportWorker;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void PrintWorker(ImportWorker w, int id) {
        System.out.println("Worker #"+id+":");
        System.out.println("\tQueries issued: "+w.getIssuedQueries());
        System.out.println("\tQueries completed: "+w.getCompletedQueries());
        System.out.println("\tQueue Length: "+w.getQueueItemsCount());
    }


    public static void main(String[] args) throws Exception {
        int workersCount = 8;
        int avgSliceLength = 2500;
        String import_file = "/tmp/dl/outside.tcpdump.json";


        System.out.println("Indexing datafile..");

        DataSlicer slicer = new DataSlicer(import_file);
        Slice[] slices = slicer.getSlicesByAvgLength(avgSliceLength);
        ArrayList<Slice[]> workerSlices = Misc.splitSlices(slices, workersCount);

        ArrayList<ImportWorker> workers = new ArrayList<ImportWorker>();
	    for(int i=0; i<workersCount; i++) {
            ImportWorker worker = new ImportWorker(import_file, workerSlices.get(i));
            worker.start();
            workers.add(worker);
        }


        long prev_issued = 0;
	    long prev_completed = 0;

	    while(true) {
	        long cur_issued = 0;
	        long cur_completed = 0;

            System.out.print("\033[H\033[2J");
            System.out.flush();

            System.out.println("Worker Status:");
            System.out.println("-------------------------");

            for(int i=0; i<workers.size(); i++) {
                ImportWorker w = workers.get(i);

                System.out.format("Worker #%2d: Queue: %5d Q_SENT: %10d Q_COMPLETE: %10d Q_COMPLETE: %3.2f%%\n", i, w.getQueueItemsCount(), w.getIssuedQueries(), w.getCompletedQueries(),  w.getQueryCompletionRate());

                cur_completed += w.getCompletedQueries();
                cur_issued += w.getIssuedQueries();
            }

            System.out.println("-------------------------");
            System.out.println((cur_completed-prev_completed)+" Queries/sec");
            prev_completed = cur_completed;
            prev_issued = cur_issued;

            TimeUnit.SECONDS.sleep(1);
        }
    }
}
