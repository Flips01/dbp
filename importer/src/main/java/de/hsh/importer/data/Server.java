package de.hsh.importer.data;
import lombok.Data;

@Data
public class Server {
    private String ip;
    private int port;

    public Server(String ip, int port){
        this.ip = ip;
        this.port = port;
    }
}
