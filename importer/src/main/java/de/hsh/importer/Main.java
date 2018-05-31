package de.hsh.importer;

import com.sun.corba.se.spi.orbutil.threadpool.Work;
import de.hsh.importer.data.Slice;
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


    public static void main(String[] args) throws InterruptedException {
        int workersCount = 8;
        String import_file = "/tmp/dl/outside.tcpdump.json";


        System.out.println("Indexing datafile..");

        DataSlicer slicer = new DataSlicer(import_file);
        Slice[] slices = slicer.getSlicesByAvgLength(2500);
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


                //System.out.println("Worker #"+i+": "+w.getProcessedItems()+" Items\t(Work Queue: "+w.getQueueItemsCount()+"\tQ_COMPLETE: "+w.getQueryCompletionRate()+"; Q_SENT: "+w.getIssuedQueries()+"; Q_COMPLETED: "+w.getCompletedQueries()+")");
                cur_completed += w.getCompletedQueries();
                cur_issued += w.getIssuedQueries();
            }

            System.out.println("-------------------------");
            System.out.println((cur_completed-prev_completed)+" Queries/sec");
            prev_completed = cur_completed;
            prev_issued = cur_issued;

            /*
            double percent = (double)current_inserts/slicer.getItemCount();
            percent = percent*100;
            System.out.println(String.format("%.2f", percent)+"% handled");
            */


            TimeUnit.SECONDS.sleep(1);
        }
    }
}
