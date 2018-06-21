package de.hsh.inform.dbp_project_readdata;

import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;

public class VoltPacket implements Serializable{

    private long ts;
    private long ms;
    private int length;

    private String srcIP;
    private int srcPort;
    private String dstIP;
    private int dstPort;

    private char type;

    private boolean SYN;
    private boolean FIN;

    private byte[] payload;
    private int size = 0;
    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
        this.size = payload.length;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getMs() {
        return ms;
    }

    public void setMs(long ms) {
        this.ms = ms;
    }

    public long getLength() {
        return length;
    }

    public boolean isSYN() {
        return SYN;
    }

    public void setSYN(boolean SYN) {
        this.SYN = SYN;
    }

    public boolean isFIN() {
        return FIN;
    }

    public void setFIN(boolean FIN) {
        this.FIN = FIN;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public String getSrcIP() {
        return srcIP;
    }

    public void setSrcIP(String srcIP) {
        this.srcIP = srcIP;
    }

    public int getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(int srcPort) {
        this.srcPort = srcPort;
    }

    public String getDstIP() {
        return dstIP;
    }

    public void setDstIP(String dstIP) {
        this.dstIP = dstIP;
    }

    public int getDstPort() {
        return dstPort;
    }

    public void setDstPort(int dstPort) {
        this.dstPort = dstPort;
    }

    public char getType() {
        return type;
    }

    public void setType(char type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "VoltPacket{" +
                "ts=" + new Date(ts*1000+ms/1000) + //import java date instead of sql date for exact time
                ", ms=" + ms +
                ", length=" + length +
                ", srcIP='" + srcIP + '\'' +
                ", srcPort=" + srcPort +
                ", dstIP='" + dstIP + '\'' +
                ", dstPort=" + dstPort +
                ", SYN=" + SYN +
                ", FIN=" + FIN+'}';
    }
}
