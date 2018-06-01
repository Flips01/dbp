package de.hsh.importer.helper;

import java.util.HashMap;
import java.util.HashSet;

public class SingleInsertManager {
    private HashSet<String> data;

    public SingleInsertManager() {
        this.data = new HashSet<String>();
    }

    public boolean canInsert(Object ... args) {
        String key = "";
        for(Object arg : args) {
            key += arg.toString();
            key += "::::";
        }
        if(this.data.contains(key)) {
            return false;
        }
        this.data.add(key);
        return true;
    }
    //private HashMap<String,>
}
