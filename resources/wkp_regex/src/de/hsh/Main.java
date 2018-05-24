package de.hsh;

public class Main {

    public static void main(String[] args) {
        boolean testF = WKPQuery.getInstance().isWKP(9999);
        boolean testT = WKPQuery.getInstance().isWKP(21);

        System.out.println("TestF="+testF);
        System.out.println("TestT="+testT);
    }
}
