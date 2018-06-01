package de.hsh.importer.data;
import com.google.gson.annotations.SerializedName;
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
    @SerializedName("size")
    private int payloadSize;

    public long msFromEpoch() {
        long _ms = (long)this.ts;
            _ms = _ms*1000;
            _ms += this.ms;
        return _ms;
    }

    public String getFlag() {
        if(this.SYN)
            return "S";
        if (this.FIN)
            return "F";
        return "";
    }
}
