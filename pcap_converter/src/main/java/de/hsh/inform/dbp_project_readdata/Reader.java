package de.hsh.inform.dbp_project_readdata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.util.ArrayList;

public class Reader {
    public static void main(String[] args) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File("out/data.bin")));
            ArrayList<VoltPacket> packets = (ArrayList<VoltPacket>)ois.readObject();
            Gson out = new GsonBuilder().registerTypeHierarchyAdapter(byte[].class, new ByteDataSerializer()).create();
            out.toJson(packets, new FileWriter(new File("out/data.json")));
            System.out.println("done");
            //packets.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


}
