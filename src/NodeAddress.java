/**
 * NodeAddress class
 */

public class NodeAddress {
    private String ip;
    private int port;

    /* Constructor */
    public NodeAddress(String i, int p) {
        ip = i;
        port = p;
    }

    /* Getters */
    String getIp() {
        return ip;
    }

    int getPort() {
        return port;
    }
}