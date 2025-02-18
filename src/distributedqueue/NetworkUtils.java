package distributedqueue;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility class for networking functionalities.
 */
public class NetworkUtils {

    /**
     * Get the local host address (LAN IPv4 if possible).
     */
    public static String getLocalHostAddress() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            return localHost.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return "127.0.0.1";
    }

    /**
     * Basic text protocol used for requests/responses. We keep it simple:
     *   type|param1|param2|...
     * For a data replication: "REPLICATE|<queueName>|<data>"
     * For create queue:       "CREATE|<queueName>"
     * For append data:        "APPEND|<queueName>|<data>"
     * For read data:          "READ|<queueName>|<clientId>"
     * For snapshot request:   "SNAPSHOT"
     * For follower register:  "REGISTER|<followerHost>|<followerPort>"
     */
    public static final String MSG_SEPARATOR = "\\|";
    public static final String MSG_JOINER = "|";
}
