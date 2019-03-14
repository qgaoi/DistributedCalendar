/**
 *
 * Node class.
 * Each Node corresponds to a single user.
 */

import java.security.KeyStore;
import java.util.*;
import java.net.*;
import java.io.*;

public class WuuNode {
    
    private int nodeId;
    private int numNodes;
    private int[] ports;
    private String[] hostNames;
    private Set<EventRecord> log;
    
    private Object lock = new Object();
    private int clock;
    private String[][][] calendar;
    private int[][] T;  // 2-dimensional time table
    private Set<EventRecord> PL;  // Partial Log
    private Set<EventRecord> NE;  
    // At each receive event, a node extracts NE of which it has not yet learned from NP
    private Set<EventRecord> NP;
    // NP:={eR|eR belong to Li and there exists a node in the sending destinations k that not hasrec(Ti, eR, k)}
    private HashMap<String, Appointment> currentAppts;  // dictionary (Vi in the algorithm), key: apppointment ID
    private boolean[] sendFail;  // keep track of sending message
    
    private int apptNo;  
    // For appointment id. The number of appointments that are created by this 
    // node, increment the number after creating a new Appointment
    
    private String nodeStateFile;
    // Calendar element: null (default) means vacant; or appointment ID
    private static final String CALENDAR_VACANT = null;
    // Simplify as a calendar which spans 7 days and in 30 minute increments.
    private static final int CALENDAR_DAYS = 7;
    private static final int CALENDAR_TIMESLOTS = 48;
    // Event Record operations:
    //private static final String ER_OP_INSERT = "insert";
    //private static final String ER_OP_DELETE = "delete";
    // Send/Receive message operations:
    private static final int MSG_SEND_LOG = 0;
    private static final int MSG_DELETE_CONFLICT = 1;
    
    /**
     * Constructor of Node.
     * @param nodeId
     */
    public WuuNode(int nodeId) {
        this.nodeId = nodeId;
        this.numNodes = Constants.TOTAL_NODES;
        this.ports = Constants.NODE_PORTS;
        this.hostNames = Constants.NODE_HOSTNAMES;
        this.log = new HashSet<>();
        
        this.clock = 0;
        this.calendar = new String[numNodes][CALENDAR_DAYS][CALENDAR_TIMESLOTS];
        this.T = new int[numNodes][numNodes];
        this.PL = new HashSet<>();
        this.NE = new HashSet<>();
        this.NP = new HashSet<>();
        this.currentAppts = new HashMap<>();
        
        this.apptNo = 0;  
        
        this.nodeStateFile = nodeId + "node_state.txt";
        
        // Track if this node sends message to other nodes successfully
        this.sendFail = new boolean[this.numNodes];
        for (int i = 0; i < sendFail.length; i++) {
            sendFail[i] = false;
        }
        
        // For failure recovery
        restoreNodeState();
    }

    public int getNodeId() {
        return nodeId;
    }


    /**
     * Create a new appointment (the participant can be himself only).
     * Check the local copy of the calendar. If no conflicts with all participants, 
     * add the meeting to site i's calendar and the event record to site i's log. 
     * Then send a message with site i's partial log to all other participants.
     * Those participants then update their logs and calendars.
     * @param apptName Name of the appointment
     * @param dayIndex Date of the appointment.
     * @param startTimeIndex HHmm in 24 hrs and 30 minutes increment, eg. "1930"
     * @param endTimeIndex
     * @param participants
     */
    public void createNewAppointment(String apptName, int dayIndex,
            int startTimeIndex, int endTimeIndex, ArrayList<Integer> participants) {
        
        boolean conflict = false;
        
        // Check local copy of calendar for the participants' availability.
        outerloop:
        for (int participant:participants) {
            synchronized(lock) {
                for (int t = startTimeIndex; t < endTimeIndex; t++) {
                    if (this.calendar[participant][dayIndex][t] != CALENDAR_VACANT) {
                        conflict = true;
                        break outerloop;
                    }
                }
            }
        }
        
        // According to the local copy of calendar, every participant is available.
        if (!conflict) {
            String id = String.format("n%03da%04d", nodeId, apptNo);
            Appointment newAppointment = new Appointment(id,
                    apptName, dayIndex, startTimeIndex, endTimeIndex, participants, this.nodeId);
            this.apptNo++;
            
            // Add the appointment to local calendar
            for (int participant : participants) {
                synchronized(lock) {
                    for (int t = startTimeIndex; t < endTimeIndex; t++) {
                        this.calendar[participant][dayIndex][t] = newAppointment.getId();
                    }
                }
            }
            
            // Add the event record to log
            insert(newAppointment);
            
            // Send partial log to all other participants
            if (participants.size() > 1) {
                for (int participant:participants) {
                    if (participant != this.nodeId) {
                        send(participant, newAppointment, MSG_SEND_LOG);
                    }
                }
            }
        }

        System.out.println("Appointment \"" + apptName + "\" added");
    }
    
    /**
     * The user can cancel an scheduled appointment it created.
     * Update the local calendar and add the event to the log.
     * Then send message to other participants.
     * @param apptId the unique id of the appointment to be deleted
     */
    public void deleteAppointment(String apptId) {
        Appointment deletedAppt = null;
        synchronized(lock) {
            // Get the scheduled appointment which to be deleted
            for (String deleteId:this.currentAppts.keySet()) {
                if (deleteId.equals(apptId)) {
                    deletedAppt = currentAppts.get(deleteId);
                }
            }
            
            if (deletedAppt != null) {
                // Delete from dictionary and log the event
                delete(deletedAppt);
                
                // Update local calendar
                int dayIndex = deletedAppt.getDay();
                int startTimeIndex = deletedAppt.getStartTime();
                int endTimeIndex = deletedAppt.getEndTime();
                for (int participant:deletedAppt.getParticipantsId()) {
                    for (int i = startTimeIndex; i < endTimeIndex; i++) {
                        this.calendar[participant][dayIndex][i] = CALENDAR_VACANT;
                    }
                }
                
                // Send message to all other participants
                if (deletedAppt.getParticipantsId().size() > 1) {
                    for (int participant:deletedAppt.getParticipantsId()) {
                        if (participant != this.nodeId) {
                            send(participant, deletedAppt, MSG_SEND_LOG);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * From Wuu & Bernstein algorithm.
     * @param Ti Node-i's time table
     * @param eR event record
     * @param k Node k
     * @return true at Node-i if Node k has learned of the event
     */
    public boolean hasRec(int[][] Ti, EventRecord eR, int k) {
        return Ti[k][eR.getERNodeId()] >= eR.getERClock();
    }
    
    /**
     * From Wuu & Bernstein Algorithm.
     * Insert appointment into dictionary, and add the event record to log.
     * @param appointment the appointment to be added
     */
    public void insert(Appointment appointment) {
        this.clock++;
        this.T[this.nodeId][this.nodeId] = this.clock;
        EventRecord eR = new EventRecord(EventOperation.ADD, this.clock, this.nodeId, appointment);
        addToLog(eR);
        synchronized(lock) {
            PL.add(eR);
            currentAppts.put(appointment.getId(), appointment);
            saveNodeState();
        }
    }
    
    /**
     * From Wuu & Bernstein Algorithm.
     * Delete the appointment from dictionary.
     * @param appointment the appointment to be deleted
     */
    public void delete(Appointment appointment) {
        this.clock++;
        this.T[this.nodeId][this.nodeId] = this.clock;
        EventRecord eR = new EventRecord(EventOperation.DELETE, this.clock, this.nodeId, appointment);
        addToLog(eR);
        synchronized(lock) {
            PL.add(eR);
            currentAppts.entrySet().removeIf(e -> e.getKey().equals(appointment.getId()));
            saveNodeState();
        }
    }
    
    /**
     * Communicative method.
     * Case A: Wuu & Bernstein: Creates NP, then sends <NP, T> to destination node.
     * Case B: Notify the initiator node that the appointment it creates 
     *          conflicts with the schedule.
     * @param destinationNode the node to which send the message
     * @param appt for case B, the appointment that is detected conflict
     * @param message to determine which case
     */
    private void send(final int destinationNode, Appointment appt, int message) {

        //System.out.println("send");
        // For case A, update NP
        if (message == MSG_SEND_LOG) {
            this.NP.clear();
            synchronized(lock) {
                for (EventRecord eR:this.PL) {
                    if (!hasRec(this.T, eR, destinationNode)) {
                        this.NP.add(eR);
                    }
                }
                saveNodeState();
            }
        }
        
        try {
            Socket socket = new Socket(hostNames[destinationNode], ports[destinationNode]);
            //System.out.println(hostNames[destinationNode]);
            //System.out.println(ports[destinationNode]);
            OutputStream out = socket.getOutputStream();
            ObjectOutputStream objectOutput = new ObjectOutputStream(out);
            objectOutput.writeInt(message);
            
            synchronized(lock) {
                switch (message) {
                    case MSG_SEND_LOG:
                        objectOutput.writeObject(this.NP);
                        /* for(EventRecord er : this.NP) {
                            System.out.println(er.getOperation());
                        } */
                        objectOutput.writeObject(this.T);
                        break;
                        
                    case MSG_DELETE_CONFLICT:
                        objectOutput.writeObject(appt);
                }
            }
            
            objectOutput.writeInt(this.nodeId);
            objectOutput.close();
            out.close();
            socket.close();
            sendFail[destinationNode] = false;
        }
        catch (ConnectException | UnknownHostException e) {
            e.printStackTrace();
            // Create new thread to keep trying to send
            if (!sendFail[destinationNode]) { // start if not started yet
                sendFail[destinationNode] = true;
                Runnable runnable = new Runnable() {
                    public synchronized void run() {
                        while (sendFail[destinationNode]) {
                            try {
                                Thread.sleep(10000);
                                send(destinationNode, appt, message);
                            }
                            catch (InterruptedException ie) {
                                ie.printStackTrace();
                            }
                        }
                    }
                };
                new Thread(runnable).start();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }    

    /**
     * Communicative method.
     * Case A: Wuu & Bernstein: Receives <NP, T> from sender node. Update NE, 
     *      dictionary V, T, and PL.
     * Case B: Initiator of the appointment receives the conflict message. It 
     *      deletes the appointment as if explicitly cancels the appointment.
     * @param clientSocket 
     */
    public void receive(Socket clientSocket) {
        Set<EventRecord> NPk = null;
        int[][] Tk = null;
        Appointment deletedAppt = null;
        int senderNode = -1;
        int message = -1;

        
        try {
            InputStream in = clientSocket.getInputStream();
            ObjectInputStream objectInput = new ObjectInputStream(in);
            message = objectInput.readInt();
            
            switch (message) {
                case MSG_SEND_LOG:
                    //System.out.println("abc");
                    NPk = (HashSet<EventRecord>)objectInput.readObject();
                    /*for(EventRecord er : NPk) {
                        System.out.println(er.getOperation());
                    }*/
                    Tk = (int[][])objectInput.readObject();
                    break;
                    
                case MSG_DELETE_CONFLICT:
                    deletedAppt = (Appointment)objectInput.readObject();
                    break;
                    
                default:
                    break;
            }
            
            senderNode = objectInput.readInt();
            objectInput.close();
            in.close();
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        
        switch (message) {
            case MSG_SEND_LOG:
                if (NPk != null) {
                    synchronized(lock) {
                        // Update NE
                        NE.clear();
                        for (EventRecord fR:NPk) {
                            if (!hasRec(this.T, fR, this.nodeId)) {
                                NE.add(fR);
                            }
                        }
                        // Update the dictionary, calendar and log
                        for  (EventRecord er:NE) {
                            addToLog(er);
                        }
                        
                        // 1) Check events in NE that to be deleted
                        HashSet<String> deleteApptIds = new HashSet<>();
                        for (String apptId:currentAppts.keySet()) {
                            for (EventRecord dR:NE) {
                                if (dR.getAppointment().getId().equals(apptId) &&dR.getOperation().equals(EventOperation.DELETE)) {
                                    deleteApptIds.add(apptId);
                                    // Update calendar
                                    int startIndex = dR.getAppointment().getStartTime();
                                    int endIndex = dR.getAppointment().getEndTime();
                                    int dayIndex = dR.getAppointment().getDay();
                                    for (Integer participant:dR.getAppointment().getParticipantsId()) {
                                        for (int i = startIndex; i < endIndex; i++) {
                                            this.calendar[participant][dayIndex][i] = CALENDAR_VACANT;
                                        }
                                    }
                                }
                            }
                        }
                        // Update dicionary
                        for (String apptId:deleteApptIds) {
                            currentAppts.remove(apptId);
                        }
                        
                        // 2) Check events in NE that to be inserted into dictionary
                        for (EventRecord er:NE) {
                            boolean deletionExists = false;
                            if (er.getOperation().equals(EventOperation.ADD)) {
                                Appointment newAppt = er.getAppointment();
                                // If there is any delete operations on this appointment, do not add
                                for (EventRecord dR:NE) {
                                    if (dR.getAppointment().getId().equals(newAppt.getId()) &&
                                        dR.getOperation().equals(EventOperation.DELETE)) {
                                        deletionExists = true;
                                    }
                                }
                                if (!deletionExists) {
                                    int startIndex = newAppt.getStartTime();
                                    int endIndex = newAppt.getEndTime();
                                    int dayIndex = newAppt.getDay();
                                    // This node is a participant. Check time conflict first.
                                    if (newAppt.getParticipantsId().contains(this.nodeId)) {
                                        boolean conflict = false;
                                        for (int t = startIndex; t < endIndex; t++) {
                                            if (this.calendar[this.nodeId][dayIndex][t] != CALENDAR_VACANT) {
                                                conflict = true;
                                            }
                                        }
                                        if (conflict) {
                                            System.out.println("The new appointment conflicts with my schedule.");
                                            // Notify senderNode that this appointment needs to be cancelled
                                            send(senderNode, newAppt, MSG_DELETE_CONFLICT);
                                            send(senderNode, newAppt, MSG_SEND_LOG);
                                        }
                                        else {
                                            // Update local dictionary and calendar
                                            currentAppts.put(newAppt.getId(), newAppt);
                                            for (int participant:newAppt.getParticipantsId()) {
                                                for (int t = startIndex; t < endIndex; t++) {
                                                    this.calendar[participant][dayIndex][t] = newAppt.getId();
                                                }
                                            }
                                        }
                                    }
                                    // This node is not a participant. Update local dicionary and calendar. No need to check conflict.
                                    else {
                                        currentAppts.put(newAppt.getId(), newAppt);
                                        for (int participant:newAppt.getParticipantsId()) {
                                            for (int t = startIndex; t < endIndex; t++) {
                                                this.calendar[participant][dayIndex][t] = newAppt.getId();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Update T
                        for (int r = 0; r < numNodes; r++) {
                            this.T[this.nodeId][r] = Math.max(this.T[this.nodeId][r], Tk[senderNode][r]);
                        }
                        for (int r = 0; r < numNodes; r++) {
                            for (int s = 0; s < numNodes; s++) {
                                this.T[r][s] = Math.max(this.T[r][s], Tk[r][s]);
                            }
                        }
                        
                        // Update PL
                        HashSet<EventRecord> removePL = new HashSet<>();
                        for (EventRecord eR:PL){
                            boolean remove = true;
                            for (int s = 0; s < numNodes; s++){
                                if (!hasRec(T, eR, s)){
                                    remove = false;
                                }
                            }
                            if (remove)
                                removePL.add(eR);
                        }

                        for (EventRecord eR:removePL) {
                            PL.remove(eR);
                        }

                        for (EventRecord eR:NE){
                            for (int s = 0; s < numNodes; s++){
                                if (!hasRec(T, eR, s)) {
                                    PL.add(eR);
                                }
                            }
                        }
                        
                        saveNodeState();
                    }
                }   
                break;
                
            // This node sent new appt to the senderNode. Then the senderNode 
            // found conflict in its schedule. This node should delete this appt, 
            // and notify all the participants.
            case MSG_DELETE_CONFLICT:
                if (deletedAppt != null) {
                    deleteAppointment(deletedAppt.getId());
                }
                break;
                
            default:
                break;
        }
    }
    
    /**
     * Add the event record to the log.
     * @param eR the EventRecord to be added to the log
     */
    private void addToLog(EventRecord eR) {
        if (!this.log.contains(eR)) {
            this.log.add(eR);
        }
    }
    
    /**
     * Save node state for recovering from failure.
     */
    private void saveNodeState() {
        try {
            FileOutputStream fos = new FileOutputStream(nodeStateFile);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            synchronized(lock) {
                oos.writeObject(this.clock);
                oos.writeObject(this.calendar);
                oos.writeObject(this.T);
                oos.writeObject(this.PL);
                oos.writeObject(this.NP);
                oos.writeObject(this.NE);
                oos.writeObject(this.currentAppts);
                oos.writeObject(this.apptNo);
            }
                oos.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Restore node state for recovering from failure.
     */
    private void restoreNodeState() {
        try {
            FileInputStream fis = new FileInputStream(nodeStateFile);
            ObjectInputStream ois = new ObjectInputStream(fis);
            this.clock = (int) ois.readObject();
            this.calendar = (String[][][]) ois.readObject();
            this.T = (int[][]) ois.readObject();
            this.PL = (Set<EventRecord>) ois.readObject();
            this.NP = (Set<EventRecord>) ois.readObject();
            this.NE = (Set<EventRecord>) ois.readObject();
            this.currentAppts = (HashMap<String, Appointment>) ois.readObject();
            this.apptNo = (int) ois.readObject();
        }

        catch (FileNotFoundException fnfe) {
            saveNodeState();
        }

        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public String[][][] getCalendar() {
        return this.calendar;
    }

    public void displayCalendarAllByAppt() {
        for (int nodeId = 0; nodeId < Constants.NODE_COUNT; ++nodeId) {
            displayCalendarByAppt(nodeId);
        }
    }

    public void displayCalendarByAppt(int nodeId) {
        System.out.println("WuuNode: " + nodeId);
        for (int day = 0; day < Constants.TOTAL_DAY; ++day) {
            System.out.println("------- " + Constants.DAYS_OF_WEEK.get(day) + " ------");
            String prevApptId = "";
            for (int slot = 0; slot < Constants.SLOT_PER_DAY; ++slot) {
                String apptId = getCalendar()[nodeId][day][slot];
                if (apptId != null && !apptId.equals(prevApptId)) {
                    prevApptId = apptId;
                    Appointment appt = currentAppts.get(apptId);
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

}
