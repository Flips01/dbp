package de.hsh.importer;

import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import de.hsh.importer.data.Package;
import de.hsh.importer.data.Slice;

public class DataReader extends Thread {
    private int batchSize;
    private String file;
    private Slice[] slices;
    private final BlockingQueue<Package[]> workQueue;
    private final ArrayList<Package> queueBuffer;
    private boolean isFinished;

    //private BlockingQueue<>
    public DataReader(String file, Slice[] slices) {
        this(file, slices, 25, 250);
    }

    public DataReader(String file, Slice[] slices, int batchSize, int queueMaxSize) {
        this.isFinished = false;
        this.file = file;
        this.slices = slices;
        this.batchSize = batchSize;
        this.workQueue = new LinkedBlockingQueue<Package[]>(queueMaxSize);
        this.queueBuffer = new ArrayList<Package>();
    }

    private void buffer(Package p) {
        this.queueBuffer.add(p);

        if(this.queueBuffer.size() == this.batchSize) {
            this.flushBuffer();
        }
    }

    private void flushBuffer() {
        if(this.queueBuffer.size() > 0) {
            Package[] arr = this.queueBuffer.toArray(new Package[this.queueBuffer.size()]);
            try {
                this.workQueue.put(arr);
            } catch (InterruptedException e) {}
            this.queueBuffer.clear();
        }
    }

    public void run(){
        this.isFinished = false;
        int current_slice = 0;
        long itemIndex = 0;

        try {
            JsonReader reader = new JsonReader(new FileReader(this.file));
            Gson gson = new GsonBuilder().create();


            reader.beginArray();
            while (reader.hasNext()) {
                // Slices berücksichtigen!
                Slice s = this.slices[current_slice];
                if(itemIndex >= s.getEnd()) {
                    current_slice++;
                    if(current_slice >= this.slices.length)
                        break;
                    s = this.slices[current_slice];
                }
                if(itemIndex < s.getStart()) {
                    reader.skipValue();
                    itemIndex++;
                    continue;
                }

                Package p = gson.fromJson(reader, Package.class);
                this.buffer(p);
                itemIndex++;
            }
            this.flushBuffer();

            reader.close();
        }
        catch (UnsupportedEncodingException ex) { }
        catch (IOException ex) { }

        this.isFinished = true;
    }

    public int queuedItems() {
        return this.workQueue.size();
    }

    public Package[] getBatch() {
        try {
            return this.workQueue.take();
        } catch (InterruptedException e) { }
        return new Package[]{};
    }

    public boolean isFinished() {
        return isFinished && this.queuedItems() == 0;
    }
}
