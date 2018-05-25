package de.hsh.importer;
import lombok.Data;

import java.util.Date;

@Data
public class Package {
    private int ts;
    private int ms;
    private int length;
    private String srcIP;
    private int srcPort;
    private String dstIP;
    private int dstPort;
    private String type;
    private boolean SYN;
    private boolean FIN;
    private String payload;
    private int size;

    public long msFromEpoch() {
        long microseconds = this.ts*1000000;
             microseconds += this.ms;
        return microseconds;
    }

    public String getFlag() {
        if(this.SYN)
            return "S";
        if (this.FIN)
            return "F";
        return "";
    }
}
