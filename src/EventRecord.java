/**
 * EventRecord class
 */

import java.io.Serializable;

public class EventRecord implements Serializable {
    private EventOperation operation;
    private int clock;
    private int nodeId;
    private Appointment appointment;

    /* Constructor */
    public EventRecord(EventOperation op, int event_clock, int node_id,
                       Appointment appt) {
        operation = op;
        clock = event_clock;
        nodeId = node_id;
        appointment = appt;
    }

    /* Getters */
    EventOperation getOperation() {
        return operation;
    }

    Appointment getAppointment() {
        return appointment;
    }


    public int getERClock() {
        return this.clock;
    }

    public int getERNodeId() {
        return this.nodeId;
    }

}
