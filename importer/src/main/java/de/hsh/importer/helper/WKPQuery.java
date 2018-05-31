package de.hsh.importer.helper;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WKPQuery {
    private final int CACHE_WKP_LIST_SECONDS = 3600;

    private static WKPQuery instance;

    private WKPQuery () {}

    public static synchronized WKPQuery getInstance () {
        if (WKPQuery.instance == null) {
            WKPQuery.instance = new WKPQuery();
        }
        return WKPQuery.instance;
    }

    ////////////////////////////////////////////////////////////////////////////

    private long wkpListTS = 0;
    private int[] wkpList;

    private int[] generateWKPList() {
        ArrayList<String> ports = new ArrayList<String>();
        Pattern p = Pattern.compile("[^\\s]*\\s*([\\d]+)/[^\\s]*.*");

        File file = new File("/etc/services");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher matcher = p.matcher(line);
                while (matcher.find()) {
                    if(matcher.groupCount() == 1) {
                        ports.add(matcher.group(1));
                    }
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

        int[] intPorts = new int[ports.size()];
        for(int i=0; i<intPorts.length; i++) {
            intPorts[i] = Integer.parseInt(ports.get(i));
        }

        return intPorts;
    }

    public int[] getWKPList() {
        long s_passed = ((System.currentTimeMillis()-this.wkpListTS)/1000);

        if (s_passed > this.CACHE_WKP_LIST_SECONDS) {
            synchronized (this) {
                if (s_passed > this.CACHE_WKP_LIST_SECONDS) {
                    this.wkpList = this.generateWKPList();
                    this.wkpListTS = System.currentTimeMillis();
                }
            }
        }

        return this.wkpList;
    }

    public boolean isWKP(int port) {
        int[] list = this.getWKPList();
        for(int x : list) {
            if(x == port)
                return true;
        }
        return false;
    }
}
