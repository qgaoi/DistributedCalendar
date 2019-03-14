/**
 * Constants class: keeps all the constant variables
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import static java.util.Map.entry;
import java.util.logging.Level;

public class Constants {
    public static final Map<Integer, NodeAddress> NODEID_ADDR_MAP =
            Map.ofEntries(
                    entry(0, new NodeAddress("Localhost", 5001)),
                    entry(1, new NodeAddress("Localhost", 5002)),
                    entry(2, new NodeAddress("Localhost", 5003))
            ); 

    public static final String[] NODE_HOSTNAMES = new String[] {
            "localhost", "localhost", "localhost"
        };

    public static final int[] NODE_PORTS = new int[] {
            5004, 5005, 5006
        };
        
    /*
    public static final Map<Integer, NodeAddress> NODEID_ADDR_MAP =
            Map.ofEntries(
                    entry(0, new NodeAddress("3.16.108.31", 5001)),
                    entry(1, new NodeAddress("18.188.161.173", 5002)),
                    entry(2, new NodeAddress("18.218.48.244", 5003))
            );

    public static final String[] NODE_HOSTNAMES = new String[] {
            "3.16.108.31", "18.188.161.173", "18.218.48.244"
    };

    public static final int[] NODE_PORTS = new int[] {
            5004, 5005, 5007
    };*/


    public static final ArrayList<String> DAYS_OF_WEEK = new ArrayList<>(
            Arrays.asList("Sunday", "Monday", "Tuesday", "Wednesday",
                    "Thursday", "Friday", "Saturday")
    );


    public static final int TOTAL_NODES = NODE_HOSTNAMES.length;

    public static final String START_DATE_CALENDAR = "20190301";

    public static final int NODE_COUNT = NODEID_ADDR_MAP.size();
    public static final int MAJORITY_COUNT = NODE_COUNT / 2 + 1;

    public static final String CALENDAR_FILENAME = "calendar.ser";
    public static final String EVENTRECORD_FILENAME = "log.ser";
    public static final int TOTAL_DAY = 7;
    public static final int SLOT_PER_DAY = 48;

    public static final int PREPARE_ID_INCREMENT = NODE_COUNT;
    public static final int WAIT_TIMEOUT = 5;   // Seconds
    public static final int NULL_ID = -1;

    public static final int MISSING_EVENT_BATCH_SIZE = 10;
    public static final int SLEEP_LENGTH = 5000;    // Milliseconds

    public static final Level GLOBAL_LOG_LEVEL = Level.WARNING;
}
