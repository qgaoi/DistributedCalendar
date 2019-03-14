/**
 * PaxosNode class that include PaxosNode environment variables
 */

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class PaxosNode {
    private final static Logger LG = Logger.getLogger(
            PaxosNode.class.getName());

    private int nodeId;
    private Lock lock = new ReentrantLock();
    private Map<String, Appointment> apptIdMap;
    private String[][][] globalTimetable;
    private ArrayList<EventRecord> allEvents;

    private Proposer proposer;
    private Acceptor accepter;
    private Learner learner;

    private int localApptId;

    /* Constructor: initialize the environment */
    public PaxosNode(int id) {
        LG.setLevel(Constants.GLOBAL_LOG_LEVEL);
        nodeId = id;
        try {
            deserializeEvents();
            deserializeCalendar();
        } catch (Exception e) {
            LG.severe("Failed to deserialize past events/calendar, exit");
            System.exit(1);
        }
        proposer = new Proposer(nodeId);
        accepter = new Acceptor(nodeId);
        learner = new Learner(this);

        lock.lock();
        localApptId = allEvents.size() + 1;
        lock.unlock();
    }

    /**
     *  Destructor */
    public void close() {
        LG.info("PaxosNode closing");
        try {
            serializeEvents();
            serializeCalendar();
        } catch (Exception e) {
            LG.warning("Serialization failure " + e);
        }
    }

    /**** Getters ****/

    public int getNodeId() {
        return nodeId;
    }

    public Map<String, Appointment> getApptIdMap() {
        lock.lock();
        Map<String, Appointment> result = apptIdMap;
        lock.unlock();
        return result;
    }

    public String[][][] getGlobalTimetable() {
        lock.lock();
        String[][][] result = globalTimetable;
        lock.unlock();
        return result;
    }

    public ArrayList<EventRecord> getAllEvents() {
        lock.lock();
        ArrayList<EventRecord> result = allEvents;
        lock.unlock();
        return result;
    }

    public Acceptor getAccepter() {
        return accepter;
    }

    public Proposer getProposer() {
        return proposer;
    }

    public Learner getLearner() {
        return learner;
    }

    /**** Setters ****/

    /**
     * addToAllEvents: add given eventRecord to given index. If there is a gap
     * between given index and current allEvent size, send out request for the
     * missing events
     * @param index
     * @param er
     * @return
     */
    public boolean addToAllEvents(int index, EventRecord er) {
        lock.lock();
        while (allEvents.size() < index) {
            int missingLogId = allEvents.size();
            allEvents.add(null);
            requestMissingEventsId(missingLogId);
        }
        if (allEvents.size() == index) {
            allEvents.add(er);
            lock.unlock();
            return true;
        }
        lock.unlock();
        return false;
    }

    private PaxosMessage generateLearnerRequest(int log_id) {
        PaxosMessage requestMsg = new PaxosMessage(
                PaxosMessageType.LEARNER_REQUEST, -1, log_id, -1,
                nodeId, null);
        return requestMsg;
    }

    /**
     * updateCalendar: provide a method for Learner to call to update calendar
     * with given EventRecord object that is already under consensus
     * @param er
     */
    public void updateCalendar(EventRecord er) {
        Appointment appt = er.getAppointment();
        // System.out.println("appt = " + appt);
        switch (er.getOperation()) {
            case ADD:
                insertAppointment(appt);
                break;
            case DELETE:
                removeAppointment(appt);
                break;
            default:
                break;
        }
    }

    /**
     * addAppointment: add given appointment information to apptIdMap and
     * globalTimetable
     * @param name
     * @param day
     * @param start
     * @param end
     * @param p
     * @return
     */
    public boolean addAppointment(String name, int day, int start, int end,
                                   ArrayList<Integer> p) {
        String newApptId = generateNewApptId();
        LG.info("Adding new appointment");
        Appointment newAppt = new Appointment(newApptId, name, day, start, end,
                p, nodeId);
        boolean addEventResult = false;
        lock.lock();
        int eventLogId = allEvents.size();
        lock.unlock();

        while (!hasConflict(newAppt) && addEventResult == false) {
            LG.info("No conflict, start paxos");
            EventRecord newEvent = new EventRecord(EventOperation.ADD, 0,
                    nodeId, newAppt);
            int newEventLogId = allEvents.size();
            if (newEventLogId == eventLogId) {
                proposer.incrementPrepareId();
            } else {
                eventLogId = newEventLogId;
                proposer.restart();
            }
            addEventResult = proposer.initEvent(newEventLogId, newEvent);
        }

        return addEventResult;
    }

    public boolean deleteAppointment(String id) {
        lock.lock();
        if (!apptIdMap.containsKey(id)) {
            lock.unlock();
            return false;
        }
        Appointment deleteAppt = apptIdMap.get(id);
        boolean removeEventResult = false;
        int eventLogId = allEvents.size();

        while (apptIdMap.containsKey(id) && removeEventResult == false) {
            EventRecord newEvent = new EventRecord(EventOperation.DELETE, 0,
                    nodeId, deleteAppt);
            int newEventLogId = allEvents.size();
            if (newEventLogId == eventLogId) {
                proposer.incrementPrepareId();
            } else {
                eventLogId = newEventLogId;
                proposer.restart();
            }
            removeEventResult = proposer.initEvent(newEventLogId, newEvent);
        }

        lock.unlock();
        return removeEventResult;
    }

    public void displayCalendarAllBySlot() {
        for (int nodeId = 0; nodeId < Constants.NODE_COUNT; ++nodeId) {
            displayCalendarBySlot(nodeId);
        }
    }

    public void displayCalendarBySlot(int nodeId) {
        System.out.println("PaxosNode: " + nodeId);
        System.out.printf("day/time ");
        for (int i = 0; i < Constants.SLOT_PER_DAY; ++i) {
            System.out.printf("%10s ", Integer.toString(i));
        }
        System.out.println();
        for (int i = 0; i < Constants.TOTAL_DAY; ++i) {
            System.out.printf("%8d ", i);
            for (int j = 0; j < Constants.SLOT_PER_DAY; ++j) {
                System.out.printf("%10s ", globalTimetable[nodeId][i][j]);
            }
            System.out.println();
        }
        System.out.println();
    }

    public void displayCalendarAllByAppt() {
        for (int nodeId = 0; nodeId < Constants.NODE_COUNT; ++nodeId) {
            displayCalendarByAppt(nodeId);
        }
    }

    public void displayCalendarByAppt(int nodeId) {
        System.out.println("PaxosNode: " + nodeId);
        for (int day = 0; day < Constants.TOTAL_DAY; ++day) {
            System.out.println("------- " + Constants.DAYS_OF_WEEK.get(day) + " ------");
            String prevApptId = "";
            for (int slot = 0; slot < Constants.SLOT_PER_DAY; ++slot) {
                String apptId = globalTimetable[nodeId][day][slot];
                if (apptId != null && !apptId.equals(prevApptId)) {
                    prevApptId = apptId;
                    Appointment appt = apptIdMap.get(apptId);
                    System.out.println("Appointment Name: " + appt.getName());
                    System.out.println("Appointment ID: " + apptId);
                    System.out.println("Start time: " + appt.getStartTime());
                    System.out.println("End time: " + appt.getEndTime());
                    System.out.print("Participants: ");
                    for (Integer p: appt.getParticipantsId()) {
                        System.out.print(p + "  ");
                    }
                    System.out.print("\n");
                }
            }
        }
        System.out.println();
    }

    /**
     * updateMissingEvents: send LEARNER_REQUEST messages to other nodes to
     * request event/log lines beyond current existing lines in allEvents.
     * If replies are received from other nodes, the message will be handled by
     * learner from ListenChannel thread and received log line will be added in
     * allEvents. The process will continue until no new log line is received.
     */
    public void updateMissingEvents() {
        lock.lock();
        int allEventsSize = allEvents.size();
        lock.unlock();
        int newLogId = allEventsSize;
        System.out.println("Get missing events, please wait...");
        while (true) {
            for (int i = 0; i < Constants.MISSING_EVENT_BATCH_SIZE; ++i) {
                PaxosMessage requestMsg = generateLearnerRequest(newLogId + i);
                try {
                    requestMsg.sendToAll();
                } catch (Exception e) {
                    LG.warning("Cannot send message to other peers " + e);
                }
            }
            try {
                Thread.sleep(Constants.SLEEP_LENGTH);
            } catch (Exception e) {
                System.err.println("updateMissingEvents sleep failed " + e);
            }
            lock.lock();
            int updatedAllEventsSize = allEvents.size();
            lock.unlock();
            if (updatedAllEventsSize == allEventsSize) {
                break;
            }
            allEventsSize = updatedAllEventsSize;
        }
        System.out.println("All events are update to date");
    }

    public void requestMissingEventsId(int log_id) {
        PaxosMessage requestMsg = generateLearnerRequest(log_id);
        requestMsg.sendToAll();
    }

    /**** Helper functions ****/

    /**
     * generateNewApptId: generate appointment id with the format:
     * "n<nodeId>a<localApptId>"
     * localApptId will be incremented for each call
     * @return
     */
    private String generateNewApptId() {
        String id = String.format("n%03da%04d", nodeId, localApptId);
        ++localApptId;
        return id;
    }

    /**
     * hasConflict: Check if given appt conflicts with existing appointments
     * @param appt
     * @return
     */
    private boolean hasConflict(Appointment appt) {
        int day = appt.getDay();
        int start = appt.getStartTime();
        int end = appt.getEndTime();
        ArrayList<Integer> participants = appt.getParticipantsId();
        lock.lock();
        for (Integer p: participants) {
            for (int i = start; i <= end; ++i) {
                if (globalTimetable[p][day][i] != null) {
                    lock.unlock();
                    return true;
                }
            }
        }
        lock.unlock();
        return false;
    }

    /**
     * insertAppointment: Add given appt to apptIdMap and globalTimetable
     * @param appt
     */
    private void insertAppointment(Appointment appt) {
//        System.out.println("Inserting appt");
        String apptId = appt.getId();
        int day = appt.getDay();
        int start = appt.getStartTime();
        int end = appt.getEndTime();
        ArrayList<Integer> participants = appt.getParticipantsId();
        apptIdMap.put(apptId, appt);
        for (Integer p: participants) {
            for (int i = start; i <= end; ++i) {
                globalTimetable[p][day][i] = apptId;
            }
        }
    }

    /**
     * removeAppointment: remove given appt from apptIdMap and globalTimetable
     * @param appt
     */
    private void removeAppointment(Appointment appt) {
        String apptId = appt.getId();
        int day = appt.getDay();
        int start = appt.getStartTime();
        int end = appt.getEndTime();
        ArrayList<Integer> participants = appt.getParticipantsId();
        lock.lock();
        for (Integer p: participants) {
            for (int i = start; i <= end; ++i) {
                globalTimetable[p][day][i] = null;
            }
        }
        apptIdMap.remove(apptId);
        lock.unlock();
    }

    /**
     * deserializeEvents: initialize the object variable allEvents
     */
    private void deserializeEvents() throws Exception {
        String filename = String.format(nodeId + "_" +
                Constants.EVENTRECORD_FILENAME);
        File fd = new File(filename);
        if (fd.exists()) {
            FileInputStream fileIn = null;
            try {
                fileIn = new FileInputStream(fd);
            } catch (Exception e) {
                throw new Exception("Cannot create FileInputStream object");
            }

            ObjectInputStream objIn = null;
            try {
                objIn = new ObjectInputStream(fileIn);
            } catch (Exception e) {
                throw new Exception("Cannot create ObjectInputStream");
            }

            allEvents = (ArrayList)objIn.readObject();
        } else {
            allEvents = new ArrayList<>();
        }
    }

    /**
     * deserializeCalendar: initialize the object variables apptIdMap and
     * globalTimetable
     */
    private void deserializeCalendar() throws Exception {
        globalTimetable = new String[3][Constants.TOTAL_DAY][Constants.SLOT_PER_DAY];
        apptIdMap = new HashMap<>();

        String filename = String.format(nodeId + "_" +
                Constants.CALENDAR_FILENAME);
        File fd = new File(filename);
        if (fd.exists()) {
            FileInputStream fileIn = null;
            try {
                fileIn = new FileInputStream(fd);
            } catch (Exception e) {
                throw new Exception("Cannot create FileInputStream object");
            }

            ObjectInputStream objIn = null;
            try {
                objIn = new ObjectInputStream(fileIn);
            } catch (Exception e) {
                throw new Exception("Cannot create ObjectInputStream");
            }

            apptIdMap = (Map<String, Appointment>)objIn.readObject();

            for (Map.Entry<String, Appointment> pair: apptIdMap.entrySet()) {
                Appointment appt = pair.getValue();
                String apptId = appt.getId();
                int day = appt.getDay();
                int start = appt.getStartTime();
                int end = appt.getEndTime();
                ArrayList<Integer> participants = appt.getParticipantsId();
                for (Integer p: participants) {
                    for (int i = start; i <= end; ++i) {
                        globalTimetable[p][day][i] = apptId;
                    }
                }
            }
        }
    }

    /**
     * serializeEvents: store object variable allEvents to file
     * @throws Exception
     */
    private void serializeEvents() throws Exception {
        String filename = String.format(nodeId + "_" +
                Constants.EVENTRECORD_FILENAME);
        File fd = new File(filename);
        FileOutputStream fileOut = new FileOutputStream(fd);
        ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
        objOut.writeObject(allEvents);
    }

    /**
     * serializeCalendar: store object variable apptIdMap to file
     * @throws Exception
     */
    private void serializeCalendar() throws Exception {
        String filename = String.format(nodeId + "_" +
                Constants.CALENDAR_FILENAME);
        File fd = new File(filename);
        FileOutputStream fileOut = new FileOutputStream(fd);
        ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
        objOut.writeObject(apptIdMap);
    }
}
