/**
 * Learner class
 */

import java.util.*;
import java.util.logging.Logger;

public class Learner {
    private final static Logger LG = Logger.getLogger(
            Learner.class.getName());
    private PaxosNode node;
    private int nodeId;

    /* Constructor */
    public Learner(PaxosNode node_obj) {
        node = node_obj;
        nodeId = node_obj.getNodeId();
        LG.setLevel(Constants.GLOBAL_LOG_LEVEL);
    }

    /**
     * handleLearnerNotice: Given msg, update allEvents and calendar stored in
     * current node
     * @param msg of type LEARNER_NOTICE
     */
    public void handleLearnerNotice(PaxosMessage msg) {
        int logId = msg.getLogId();
        EventRecord er = msg.getER();
        LG.info("handleLearnerNotice er = " + er);
        try {
            node.addToAllEvents(logId, er);
        } catch (Exception e) {
            LG.warning("learner addToAllEvents failed + " + e);
        }
        try {
            node.updateCalendar(er);
        } catch (Exception e) {
            LG.warning("learner updateCalendar failed + " + e);
        }
    }

    /**
     * handleLearnerRequest: The function reads the given request msg, get the
     * requested logId from current node. If the current node includes the
     * logId, reply back to the requester. Else, ignore the request.
     * @param msg of type LEARNER_REQUEST
     */
    public void handleLearnerRequest(PaxosMessage msg) {
        NodeAddress addr = Constants.NODEID_ADDR_MAP.get(msg.getNodeId());
        int requestedLogId = msg.getLogId();
        LG.info("requested logid = " + requestedLogId);

        ArrayList<EventRecord> learnedER = node.getAllEvents();
        LG.info("learnedER.size() = " + learnedER.size());
        if (requestedLogId < learnedER.size()) {
            LG.info("learnedER.get(requestedLogId) = " +
                    learnedER.get(requestedLogId));
        }
        if (learnedER.size() > requestedLogId &&
                learnedER.get(requestedLogId) != null) {
            LG.info("handleLearnerRequest sending reply");
            PaxosMessage replyMsg = new PaxosMessage(
                    PaxosMessageType.LEARNER_NOTICE, -1, requestedLogId,
                    -1, nodeId, learnedER.get(requestedLogId));
            try {
                replyMsg.sendToAddr(addr.getIp(), addr.getPort());
            } catch (Exception e) {
                LG.warning("HandleLearnerRequest replyMsg failed " +
                        e);
            }
        }
    }
}