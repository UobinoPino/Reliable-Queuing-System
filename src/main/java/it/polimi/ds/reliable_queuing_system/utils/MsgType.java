package it.polimi.ds.reliable_queuing_system.utils;

public enum MsgType {
    //CLIENT CONNECTION
    request_client_id,
    assign_client_id,

    // READ/WRITE OPERATIONS
    read_req,
    update_prop,
    prop_ack,
    read_res,
    write_req,
    write_res,

    // FOLLOWER BROKER CRASHnew
    heartbeat,
    heartbeat_ack,
    rm_broker,

    // LEADER BROKER CRASH
    election,
    election_ack,
    new_leader,

    // BROKER CONNECTION
    join_req,
    new_broker,
    new_broker_ack,
    state_prop,
}
