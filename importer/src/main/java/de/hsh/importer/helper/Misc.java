package de.hsh.importer.helper;

import de.hsh.importer.data.Server;
import de.hsh.importer.data.Slice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class Misc {
    public static ArrayList<Slice[]> splitSlices(Slice[] slices, int n){
        ArrayList<ArrayList<Slice>> data = new ArrayList<ArrayList<Slice>>();

        // initialisieren
        for(int i=0; i<n; i++) {
            data.add(new ArrayList<Slice>());
        }

        for(int i=0; i<slices.length; i++) {
            int arl = i%n;
            data.get(arl).add(slices[i]);
        }

        ArrayList<Slice[]> results = new ArrayList<Slice[]>();
        for(int i=0; i<data.size(); i++) {
            ArrayList<Slice> _slices = data.get(i);
            results.add(_slices.toArray(new Slice[_slices.size()]));
        }

        return results;
    }

    public static Server[] getServers() {
        return new Server[]{
                new Server("192.168.58.2", 21212),
                new Server("192.168.58.3", 21212),
                new Server("192.168.58.4", 21212),
                new Server("192.168.58.5", 21212)
        };
    }

    public static Server[] getServersRandomOrder() {
        Server[] server = getServers();
        ArrayList<Server> serverAL = new ArrayList<Server>(Arrays.asList(server));
        Collections.shuffle(serverAL);
        return serverAL.toArray(new Server[server.length]);
    }

    public static int partitionTs(int ts, int partitionInterval) {
        int p = ts / partitionInterval;
        return p*partitionInterval;
    }

    public static int[] partitionsInRange(int start, int end, int partitionInterval) {
        int start_part = partitionTs(start, partitionInterval);
        int end_part = partitionTs(end, partitionInterval);

        int part_range = end_part-start_part;
        int partitions = (part_range/partitionInterval)+1;

        int[] parts = new int[partitions];
        for(int i=0; i<partitions; i++) {
            parts[i] = start_part+ (i*partitionInterval);
        }
        return parts;
    }
}
