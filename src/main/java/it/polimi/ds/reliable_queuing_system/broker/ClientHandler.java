package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.utils.MsgType;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ClientHandler implements Runnable {
    public ClientHandler(Socket socket, int clientId) throws IOException {
        this.fromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.toClient = new PrintWriter(socket.getOutputStream(), true);
        this.clientId = clientId;
    }

    private final BufferedReader fromClient;
    private final PrintWriter toClient;
    private final int clientId;

    @Override
    public void run() {
        try {
            // keep waiting for incoming messages from the client
            String msg;
            JSONObject msgObj;
            while ((msg = fromClient.readLine()) != null) {
                msgObj = new JSONObject(msg);
                MsgType msgType = MsgType.valueOf(msgObj.getString("type"));
                switch (msgType) {
                    case MsgType.request_client_id -> assignClientId(msgObj);
                    //TODO: invoke methods to handle each type of message from client
                    case MsgType.read_req -> tmpHandleRead(msgObj);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();  //TODO: handle better?
        }
    }

    private void assignClientId(JSONObject req) {
        JSONObject res = new JSONObject();
        res.put("type", MsgType.assign_client_id);
        res.put("id", clientId);
        toClient.println(res);
    }

    private void tmpHandleRead(JSONObject reqObj) {
        String queueId = reqObj.getString("queue_id");
        JSONObject res = new JSONObject();
        res.put("type", MsgType.read_res);
        res.put("client_id", clientId);
        List<Integer> values = new ArrayList<>();
        if (queueId.equals("a")) {
            values.add(1);
            values.add(2);
        }
        res.put("values", values);

        toClient.println(res);
    }
}
