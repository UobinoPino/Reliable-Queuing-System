package it.polimi.ds.reliable_queuing_system.utils;

import java.io.ObjectOutputStream;
import java.net.Socket;

public record PooledConnection(
        Socket socket,
        ObjectOutputStream outStream
) {
}
