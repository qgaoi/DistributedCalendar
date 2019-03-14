/**
 * Proposer class
 */

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import static java.lang.System.exit;

public class Proposer {
    private final static Logger LG = Logger.getLogger(
            Proposer.class.getName());

    private int nodeId;
    private int logId;
    private int prepareId;
    private EventRecord targetVal;
    private boolean targetValAccepted;

    private Lock valuesLock;
    private int maxPromisedId;
    private EventRecord receivedVal;
    private int promiseCount;
    private int acceptCount;

    private final Lock promiseMajorityLock;
    private final Condition promiseMajority;
    private final Lock acceptMajorityLock;
    private final Condition acceptMajority;

    /* Constructor */
    public Proposer(int node_id) {
        nodeId = node_id;
        prepareId = node_id;
        maxPromisedId = -1;
        targetVal = null;
        receivedVal = null;
        targetValAccepted = true;
        promiseCount = 0;
        acceptCount = 0;
        valuesLock = new ReentrantLock();

        promiseMajorityLock = new ReentrantLock();
        promiseMajority = promiseMajorityLock.newCondition();

        acceptMajorityLock = new ReentrantLock();
        acceptMajority = acceptMajorityLock.newCondition();

        LG.setLevel(Constants.GLOBAL_LOG_LEVEL);
    }

    public void restart() {
        prepareId = nodeId;
        targetValAccepted = true;
        maxPromisedId = -1;
        receivedVal = null;
        promiseCount = 0;
        acceptCount = 0;
    }

    public boolean initEvent(int log_id, EventRecord er) {
        LG.info("initEvent " + log_id);
        logId = log_id;
        targetVal = er;

        prepare();
        promiseMajorityLock.lock();
        try {
            boolean getMajorityPromise = promiseMajority.await(
                    Constants.WAIT_TIMEOUT, TimeUnit.SECONDS);
            if (getMajorityPromise == false) {
                LG.info("initEvent failed to get majority promise");
                return false;
            }
            // promiseMajority.await();
            LG.info("initEvent got the majority promise");
        } catch (Exception e) {
            exit(1);
        } finally {
            promiseMajorityLock.unlock();
        }

        LG.info("Next step: propose");

        propose();
        acceptMajorityLock.lock();
        try {
            boolean getMajorityAccept = acceptMajority.await(
                    Constants.WAIT_TIMEOUT, TimeUnit.SECONDS);
            if (getMajorityAccept == false) {
                LG.info("initEvent failed to get majority accept");
                return false;
            }
            LG.info("initEvent got the majority accept");
        } catch (Exception e) {
            exit(1);
        } finally {
            acceptMajorityLock.unlock();
        }

        PaxosMessage learnerNoticeMsg = new PaxosMessage(
                PaxosMessageType.LEARNER_NOTICE, prepareId, log_id,
                Constants.NULL_ID, nodeId, receivedVal == null ?
                targetVal : receivedVal);
        try {
            learnerNoticeMsg.sendToAll();
        } catch (Exception e) {
            System.err.println("Send learner notice failed " + e);
        }

        restart();
        return targetValAccepted;
    }

    /**
     * prepare: send out prepare message to all the peers
     */
    private void prepare() {
        PaxosMessage msg = new PaxosMessage(PaxosMessageType.PREPARE, prepareId,
                logId, -1, nodeId, null);
        try {
            msg.sendToAll();
        } catch (Exception e) {
            System.err.println("Send prepare message failed " + e);
        }
    }

    /**
     * propose: send out propose message to all the peers
     * If proposer has received any value back from acceptor in prepare phase,
     * set the receivedVal as propose value;
     * Else, use the targetVal as propose value
     */
    public void propose() {
        if (receivedVal != null) {
            targetValAccepted = false;
        }
        PaxosMessage msg = new PaxosMessage(PaxosMessageType.PROPOSE, prepareId,
                logId, -1, nodeId,
                receivedVal == null ? targetVal : receivedVal);
        LG.info("Sending proposals");
        try {
            msg.sendToAll();
        } catch (Exception e) {
            LG.warning("send proposal failed " + e);
        }
    }

    public void handlePromise(PaxosMessage msg) {
        int prepare_id = msg.getPId();

        /* If the promise message is not for the current prepareId, discard */
        if (prepare_id != prepareId) {
            return;
        }

        int promised_id = msg.getPromisedId();
        if (promised_id > maxPromisedId) {
            maxPromisedId = promised_id;
            receivedVal = msg.getER();
        }

        valuesLock.lock();
        ++promiseCount;
        LG.info("promiseCount = " + promiseCount);

        promiseMajorityLock.lock();
        if (promiseCount >= Constants.MAJORITY_COUNT) {
            LG.info("got the majority promise");
            promiseMajority.signal();
        } else {
            LG.info("Not got the majority promise yet");
        }
        valuesLock.unlock();
        promiseMajorityLock.unlock();
    }

    public void handleAccept(PaxosMessage msg) {
        int prepare_id = msg.getPId();

        /* If the promise message is not for the current prepareId, discard */
        if (prepare_id != prepareId) {
            return;
        }

        valuesLock.lock();
        ++acceptCount;
        acceptMajorityLock.lock();
        if (acceptCount >= Constants.MAJORITY_COUNT) {
            acceptMajority.signal();
        }
        valuesLock.unlock();
        acceptMajorityLock.unlock();
    }

    /** Helper functions **/
    public void incrementPrepareId() {
        prepareId += Constants.PREPARE_ID_INCREMENT;
        promiseCount = 0;
    }
}
