import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Logger;

import static java.lang.System.exit;

/**
 * Created by qigao on 2019/3/13.
 */

public class Main {

    private final static Logger LG = Logger.getLogger(
            Main.class.getName());


    public static void main(String args[]) {
        LG.setLevel(Constants.GLOBAL_LOG_LEVEL);
        String algo = args[0];
        int nodeID = -1;
        try {
            nodeID = parseArgs(args);
        } catch (Exception e) {
            System.err.println(e);
            exit(1);
        }

        if(algo.equals("wuu")) {
            wuuBernsteinMain(nodeID);
        }

        if(algo.equals("paxos")) {
            paxosMain(nodeID);
        }

    }


    public static void wuuBernsteinMain(int nodeID) {

        int port = Constants.NODE_PORTS[nodeID];

        WuuNode node = new WuuNode(nodeID);

        Runnable listenThread = new Runnable(){
            public synchronized void run() {
                System.out.println("Start listening for other nodes");
                ServerSocket serverSocket;
                try {
                    serverSocket = new ServerSocket(port);
                    while (true) {
                        final Socket client = serverSocket.accept();
                        Runnable runnable = new Runnable() {
                            public synchronized void run() {
                                node.receive(client);
                            }
                        };
                        new Thread(runnable).start();

                    }
                }
                catch (IOException e) {
                    System.out.println("Exception caught when trying to listen on port " + port);
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }
            }
        };
        new Thread(listenThread).start();

        while(true){
            @SuppressWarnings("resource")
            Scanner commandLine = new Scanner(System.in);

            String input = null;

            if (commandLine.hasNextLine()) {
                input = commandLine.nextLine();
            }
            System.out.println(input);
            handleCommandWuu(input, node);
        }
    }

    public static void paxosMain(int nodeID) {

        LG.info("Node id = " + nodeID);
        PaxosNode node = new PaxosNode(nodeID);

        /* Create listen thread */
        int port = Constants.NODEID_ADDR_MAP.get(nodeID).getPort();
        ServerSocket server = null;
        try {
            server = new ServerSocket(port);
        } catch (Exception e) {
            LG.severe("Cannot create server socket");
            exit(1);
        }
        ListenChannel listenThread = new ListenChannel(server, node);
        listenThread.start();

        /* Pick up records that might have been missed before node start */
        node.updateMissingEvents();

        /* Start to take user input */
        Scanner sc = new Scanner(System.in);
        boolean endProgram = false;
        while (sc.hasNextLine() && endProgram == false) {
            String input = sc.nextLine();
            handleCommand(input, node);
        }
        System.out.println("Ending the program");
        node.close();
        System.out.println("Program ended");
    }

    private static void handleCommand(String input, PaxosNode node) {
        Scanner sc = new Scanner(input);
        String operation = null;
        if (sc.hasNext()) {
            operation = sc.next();
            LG.info("operation = " + operation);
        } else {
            return;
        }
        switch (operation) {
            case "add":
                handleAddCommand(sc, node);
                break;
            case "delete":
                handleDeleteCommand(sc, node);
                break;
            case "view":
                handleViewCommand(sc, node);
                break;
            case "exit":
                node.close();
                exit(0);
                break;
            default:
                handleInvalidCommand();
                break;
        }
    }

    private static void handleCommandWuu(String input, WuuNode node) {
        Scanner in = new Scanner(input);
        String operation = null;
        String apptName;

        if(in.hasNext()) {
            operation = in.next();
        }

        if (operation.equals("add")) {
            int start;
            int end;
            int apptDay;
            ArrayList<Integer> participants = new ArrayList<Integer>();

            if (in.hasNext()) {
                apptName = in.next();
            } else {
                System.out.println("Invalid appointment name");
                return;
            }

            if (in.hasNextInt()) {
                apptDay = in.nextInt();
            } else {
                System.out.println("Invalid appointment day");
                return;
            }

            if (in.hasNextInt()) {
                start = in.nextInt();
            } else {
                System.out.println("Invalid appointment start time");
                return;
            }

            if (in.hasNextInt()) {
                end = in.nextInt();
            } else {
                System.out.println("Invalid appointment end time");
                return;
            }

            while (in.hasNextInt()) {
                participants.add(Integer.valueOf(in.next()));
            }

            node.createNewAppointment(apptName, apptDay, start, end,
                    participants);

        }
        else if (operation.equals("delete")) {
            if (!in.hasNext()) {
                System.out.println("Appointment id to delete is missing.");
                return;
            }
            String apptDeleteId = in.next();
            node.deleteAppointment(apptDeleteId);

        }
        else if (operation.equals("view")) {
            if (in.hasNext()) {
                node.displayCalendarAllByAppt();
            } else {
                node.displayCalendarByAppt(node.getNodeId());
            }
        }
        else {
            handleInvalidCommand();
        }
    }

    private static void handleDeleteCommandWuu(Scanner sc, WuuNode node) {
        if (!sc.hasNext()) {
            System.out.println("Appointment id to delete is missing.");
            return;
        }
        String apptDeleteId = sc.next();
        node.deleteAppointment(apptDeleteId);
    }


    private static void handleDeleteCommand(Scanner sc, PaxosNode node) {
        if (!sc.hasNext()) {
            System.out.println("Appointment id to delete is missing.");
            return;
        }
        String apptDeleteId = sc.next();
        boolean deleteResult = node.deleteAppointment(apptDeleteId);
        if (!deleteResult) {
            System.out.println("Appointment does not exist");
        } else {
            System.out.println("Appointment " +  apptDeleteId + " is deleted");
        }
    }

    private static void handleViewCommand(Scanner sc, PaxosNode node) {
        if (sc.hasNext()) {
            node.displayCalendarAllByAppt();
        } else {
            node.displayCalendarByAppt(node.getNodeId());
        }
    }

    private static void handleAddCommand(Scanner sc, PaxosNode node) {
        String apptName = null;
        if (sc.hasNext()) {
            apptName = sc.next();
        } else {
            System.out.println("Invalid appointment name");
            return;
        }

        int day = -1;
        if (sc.hasNextInt()) {
            day = sc.nextInt();
        } else {
            System.out.println("Invalid appointment day");
            return;
        }

        int start = -1;
        if (sc.hasNextInt()) {
            start = sc.nextInt();
        } else {
            System.out.println("Invalid appointment start time");
            return;
        }

        int end = -1;
        if (sc.hasNextInt()) {
            end = sc.nextInt();
        } else {
            System.out.println("Invalid appointment end time");
            return;
        }

        ArrayList<Integer> participants = new ArrayList<>();
        if (sc.hasNextInt()) {
            participants.add(sc.nextInt());
            while (sc.hasNextInt()) {
                participants.add(Integer.valueOf(sc.nextInt()));
            }
        } else {
            System.out.println("Invalid appointment participant");
        }

        boolean addApptResult = node.addAppointment(apptName, day, start, end,
                participants);
        if (!addApptResult) {
            System.out.println("Appointment cannot be added because of " +
                    "conflicts");
        } else {
            System.out.println("Appointment \"" + apptName + "\" added");
        }
    }

    private static void handleInvalidCommand() {
        System.out.println("Invalid command");
    }

    /**
     * parseArgs: allows one and only one argument as nodeId
     */
    private static int parseArgs(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Incorrect argument number");
        }

        String nodeIdStr = args[1];
        if (!isNonnegInteger(nodeIdStr, 10)) {
            throw new Exception("Invalid nodeId");
        }

        int nodeId = Integer.parseInt(nodeIdStr);
        if (Constants.NODEID_ADDR_MAP.size() <= nodeId) {
            throw new Exception("Invalid nodeId");
        }
        return nodeId;
    }

    /** Helpers **/

    /**
     * isNonnegInteger: Check if given String s is a non-negative integer
     * @param s
     * @param radix
     * @return
     */
    public static boolean isNonnegInteger(String s, int radix) {
        if(s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            if (Character.digit(s.charAt(i),radix) < 0) {
                return false;
            }
        }
        return true;
    }
}


