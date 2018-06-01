package de.hsh.importer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import de.hsh.importer.data.Slice;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class DataSlicer {
    private String file;
    private long itemCount;

    public DataSlicer(String file) throws IOException {
        this.file = file;
        this.init();
    }

    private void init()  throws IOException {
        this.itemCount = 0;

        try {
            JsonReader reader = new JsonReader(new FileReader(this.file));

            reader.beginArray();
            while (reader.hasNext()) {
                reader.skipValue();
                itemCount++;
            }


            reader.close();
        }
        catch (UnsupportedEncodingException ex) {}
        catch (IOException ex) {
            throw ex;
        }
        catch (Exception ex) {}
    }

    public Slice[] getSlicesByAvgLength(long length) {
        long slices = (this.itemCount/length);
        return this.getSlices(slices);
    }

    public Slice[] getSlices(long n) {
        Slice[] slices = new Slice[(int)n];

        long slice_size = this.itemCount/n;
        for(int i=0;i<n;i++) {
            Slice slice = new Slice();
            slice.setStart(    Math.max((i*slice_size)-1, 0) );
            slice.setEnd( ((i+1)*slice_size)-1 );

            slices[i] = slice;
        }

        // sicherstellen, dass alle items gelesen werden
        slices[(int)n-1].setEnd(this.itemCount-1);

        return slices;
    }

    public long getItemCount() {
        return this.itemCount;
    }
}
