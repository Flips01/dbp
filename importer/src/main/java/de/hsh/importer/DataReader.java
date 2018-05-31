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

public class DataReader extends Thread {
    private int batchSize;
    private String file;
    private final BlockingQueue<Package[]> workQueue;
    private final ArrayList<Package> queueBuffer;

    //private BlockingQueue<>
    public DataReader(String file) {
        this(file, 25, 250);
    }

    public DataReader(String file, int batchSize, int queueMaxSize) {
        this.file = file;
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
        try {
            JsonReader reader = new JsonReader(new FileReader(this.file));
            Gson gson = new GsonBuilder().create();

            reader.beginArray();
            while (reader.hasNext()) {
                Package p = gson.fromJson(reader, Package.class);
                this.buffer(p);
            }
            this.flushBuffer();

            reader.close();
        } catch (UnsupportedEncodingException ex) {

        } catch (IOException ex) {
        }

        System.out.println("thread is running...");
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
}
