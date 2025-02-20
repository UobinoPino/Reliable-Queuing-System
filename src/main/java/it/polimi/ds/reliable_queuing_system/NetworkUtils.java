package it.polimi.ds.reliable_queuing_system;

/**
 * Holds the common network constants and operations for splitting requests.
 */
public class NetworkUtils {

    // Must be a single pipe character for correct command splitting
    public static final String MSG_SEPARATOR = "|";



    /**
     * Retrieves the local host address (e.g., "127.0.0.1" or your LAN IP).
     */
    public static String getLocalHostAddress() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }
}