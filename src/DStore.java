import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;

public class DStore {

    private final int port;
    private final int cport;
    private final int timeout;
    private final File fileFolder;
    private Socket controller;
    private Socket client;
    private ServerSocket ss;
    private boolean stable = true;

    public DStore(int port, int cport, int timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = new File(System.getProperty("user.dir") + "/dStorage/" + fileFolder);
        var res = true;
        if (this.fileFolder.exists()) {
            for (var f : Objects.requireNonNull(this.fileFolder.listFiles())) {
                res = res && f.delete();
            }
        } else {
            res = this.fileFolder.mkdir();
        }
        if (!res) {
            throw new RuntimeException("Error: dStorage Creation failed. ID: " + this.port);
        }
    }

    public static void main(String[] args) {
        new DStore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]).start();
    }

    private void listenToServer() {
        stable = true;
        new Thread(() -> {
            while (stable) {
                try {
                    var in = new BufferedReader(new InputStreamReader(controller.getInputStream()));
                    String msg;
                    while ((msg = in.readLine()) != null) handleServerMessage(msg);
                    controller.close();
                } catch (Exception e) {
                    stable = false;
                    System.err.println("Error listening to server: " + e);
                }
            }
        }).start();
    }

    private void listenForClient() {
        new Thread(() -> {
            try {
                ss = new ServerSocket(port);
                while (true) {
                    client = ss.accept();
                    var in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String msg;
                    while ((msg = in.readLine()) != null) {
                        handleClientMessage(msg);
                    }
                    client.close();
                }
            } catch (SocketException e) {
                if (e.getMessage().contains("Socket closed")) {
                    System.out.println("Client terminated connection with DStore: " + port);
                } else {
                    System.err.println("Error creating ServerSocket in DStore: " + e);
                }
            } catch (Exception e) {
                System.err.println("Error creating ServerSocket in DStore: " + e);
            }
        }).start();
    }

    public void sendMessage(String msg, Socket dest) {
        try {
            var out = new PrintWriter(dest.getOutputStream());
            out.println(msg);
            out.flush();
        } catch (IOException e) {
            System.err.println("Error sending message: " + msg + " to dest " + dest.getLocalPort());
            e.printStackTrace();
        }
    }

    private void handleServerMessage(String msg) {
        var args = msg.split(" ");

        // REMOVE commands.

        if (msg.startsWith("REMOVE")) {
            var file = new File(fileFolder + "/" + args[1]);
            System.out.println("Request to remove received: " + file.getName());
            if (file.exists()) {
                if (file.delete()) {
                    sendMessage("REMOVE_ACK " + args[1], controller);
                    System.out.println("Removed file: " + args[1]);
                } else {
                    System.err.println("Error deleting file " + args[1] + " at DStore: " + port);
                }
            } else {
                sendMessage("ERROR_FILE_DOES_NOT_EXIST " + args[1], controller);
            }
        } else if (msg.startsWith("LIST")) {
            StringBuilder reply = new StringBuilder("LIST");
            for (var t : Objects.requireNonNull(fileFolder.list())) {
                reply.append(" ").append(t);
            }
            sendMessage(reply.toString(), controller);
        }
    }

    private void handleClientMessage(String msg) {
        var args = msg.split(" ");

        // STORE commands.

        if (msg.startsWith("STORE")) {
            var file = new File(fileFolder + "/" + args[1]);
            System.out.println("Request to store received: " + args[1]);
            sendMessage("ACK", client);
            System.out.println("ACK sent for: " + args[1]);
            try {
                System.out.println("Beginning write for: " + args[1]);
                var in = client.getInputStream();
                var buf = new byte[Integer.parseInt(args[2])];
                int bufLen;
                var out = new FileOutputStream(file);
                while ((bufLen = in.read(buf)) != -1) {
                    out.write(buf, 0, bufLen);
                }
                in.close();
                out.close();
                client.close();
                System.out.println("Written file: " + args[1]);
            } catch (IOException e) {
                System.err.println("Error accepting file contents from client.");
                e.printStackTrace();
            }
            sendMessage("STORE_ACK " + args[1], controller);
        }

        // LOAD commands.

        else if (msg.startsWith("LOAD_DATA")) {
            var file = new File(fileFolder + "/" + args[1]);
            System.out.println("Request to load received: " + file.getName());
            try {
                if (file.exists()) {
                    System.out.println("Beginning load for: " + file.getName());
                    var in = new FileInputStream(file);
                    var out = client.getOutputStream();
                    var buf = new byte[(int) file.length()];
                    int bufLen;
                    while ((bufLen = in.read(buf)) != -1) {
                        out.write(buf, 0, bufLen);
                    }
                    in.close();
                    out.close();
                    System.out.println("Loaded file: " + file.getName());
                } else {
                    client.close();
                }
            } catch (Exception e) {
                System.err.println("Error loading data from Dstore " + port + ":" + e);
                e.printStackTrace();
            }
        }
    }

    public void start() {
        try {
            controller = new Socket("localhost", cport);
            sendMessage("JOIN " + port, controller);
            System.out.println("DStore " + port + " registered with server");
            listenToServer();
            listenForClient();
        } catch (Exception e) {
            System.out.println(getClass() + " error: " + e);
        }
    }
}