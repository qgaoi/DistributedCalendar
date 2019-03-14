/**
 * PaxosMessageType
 */

public enum PaxosMessageType {
    PREPARE,
    PROMISE,
    PROPOSE,
    ACCEPT,
    LEARNER_REQUEST,
    LEARNER_NOTICE
}
