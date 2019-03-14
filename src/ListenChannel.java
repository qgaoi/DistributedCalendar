/**
 * ListenChannel class for multithreading
 */

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;


public class ListenChannel extends Thread {
    private final static Logger LG = Logger.getLogger(
            ListenChannel.class.getName());

    private ServerSocket server;
    private PaxosNode node;

    /* Constructor */
    public ListenChannel(ServerSocket s, PaxosNode n) {
        server = s;
        node = n;

        LG.setLevel(Constants.GLOBAL_LOG_LEVEL);
    }

    /**
     * run: thread to listen to incoming messages
     */
    public void run() {
        while (true) {
            try {
                Socket socket = server.accept();
                ObjectInputStream ois = new ObjectInputStream(
                        socket.getInputStream());
                PaxosMessage paxosMsg = (PaxosMessage)ois.readObject();
                PaxosMessageType type = paxosMsg.getMsgType();
                LG.info("Received paxos message " + type);
                switch (type) {
                    case PREPARE:
                        node.getAccepter().handlePrepare(paxosMsg);
                        break;
                    case PROMISE:
                        node.getProposer().handlePromise(paxosMsg);
                        break;
                    case PROPOSE:
                        node.getAccepter().handlePropose(paxosMsg);
                        break;
                    case ACCEPT:
                        node.getProposer().handleAccept(paxosMsg);
                        break;
                    case LEARNER_NOTICE:
                        node.getLearner().handleLearnerNotice(paxosMsg);
                        break;
                    case LEARNER_REQUEST:
                        node.getLearner().handleLearnerRequest(paxosMsg);
                        break;
                    default:
                        break;
                }
                ois.close();
                socket.close();
            } catch (Exception e) {
                LG.warning("receiving failed " + e);
            }
        }
    }
}
