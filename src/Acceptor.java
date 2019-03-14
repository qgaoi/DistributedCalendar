/**
 * Acceptor class
 */

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Acceptor {
    private final static Logger LG = Logger.getLogger(
            ListenChannel.class.getName());

    private Map<Integer, AcceptorStore> logIdToStoreMap;
    private int nodeId;

    /* Constructor */
    public Acceptor(int node_id) {
        logIdToStoreMap = new HashMap<>();
        nodeId = node_id;
        LG.setLevel(Constants.GLOBAL_LOG_LEVEL);
    }

    /**
     * handlePrepare: handle the prepare message with pId
     * Acceptor will promise to the pId unless it has already promised to a
     * greater pId.
     * If it has already accepted a value, send the <accepted pId, value> back
     * to proposer together with the promise message.
     * @param msg
     */
    public void handlePrepare(PaxosMessage msg) {
        LG.info("Handling prepare message");
        int pId = msg.getPId();
        int logId = msg.getLogId();
        if (!logIdToStoreMap.containsKey(logId)) {
            logIdToStoreMap.put(logId, new AcceptorStore());
        }
        AcceptorStore as = logIdToStoreMap.get(logId);

        if (pId < as.promisedId) {
            return;
        }
        as.promisedId = pId;

        int acceptedId = as.acceptedId;
        EventRecord acceptedER = as.acceptedER;

        PaxosMessage promiseMsg = new PaxosMessage(PaxosMessageType.PROMISE,
                pId, msg.getLogId(), acceptedId, nodeId, acceptedER);
        int proposerId = msg.getNodeId();
        NodeAddress proposerAddr = Constants.NODEID_ADDR_MAP.get(proposerId);
        LG.info("Sending out promise msg for pId " + pId);
        promiseMsg.sendToAddr(proposerAddr.getIp(), proposerAddr.getPort());
    }

    /**
     * handlePropose: handle the propose message with <pId, value>
     * Acceptor will accept the proposed value unless it has previously
     * promised to a pId that is greater than current pId.
     * If accepted, send out an accept message. Otherwise, neglect the message.
     * @param msg
     */
    public void handlePropose(PaxosMessage msg) {
        LG.info("Handling propose message");
        int logId = msg.getLogId();
        int msgPId = msg.getPId();

        int curPromisedId = logIdToStoreMap.getOrDefault(logId,
                new AcceptorStore()).promisedId;
        if (msgPId < curPromisedId) {
            LG.info("Reject propose msg, pId = " + msgPId);
            return;
        }

        logIdToStoreMap.get(logId).acceptedId = msgPId;
        logIdToStoreMap.get(logId).promisedId = msgPId;
        logIdToStoreMap.get(logId).acceptedER = msg.getER();

        PaxosMessage acceptMsg = new PaxosMessage(PaxosMessageType.ACCEPT,
                msgPId, msg.getLogId(), msgPId, nodeId, msg.getER());
        int proposerId = msg.getNodeId();
        NodeAddress proposerAddr = Constants.NODEID_ADDR_MAP.get(proposerId);
        LG.info("Sending accept msg");
        acceptMsg.sendToAddr(proposerAddr.getIp(), proposerAddr.getPort());
    }

    private class AcceptorStore {
        private int promisedId;
        private int acceptedId;
        private EventRecord acceptedER;

        /* Constructor */
        public AcceptorStore() {
            promisedId = -1;
            acceptedId = -1;
            acceptedER = null;
        }
    }
}
