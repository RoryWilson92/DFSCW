import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;

enum State {
    STORE_IN_PROGRESS, STORE_COMPLETE, REMOVE_IN_PROGRESS, REMOVE_COMPLETE
}

public class Controller {

    private final Integer R;
    private final int timeout;
    private final int rebalancePeriod;
    private final int cport;
    private final Map<Socket, Integer> dStoreMap;
    private final Set<Socket> clients;
    private final Index index;
    private boolean open;

    public Controller(int cport, int R, int timeout, int rebalancePeriod) {
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        this.cport = cport;
        open = false;
        index = new Index();
        dStoreMap = new HashMap<>();
        clients = new HashSet<>();
    }

    public static void main(String[] args) {
        var dir = new File(System.getProperty("user.dir") + "/dStorage");
        if (dir.exists()) {
            Controller.deleteDirectory(dir);
        }
        if (!dir.mkdir()) {
            System.err.println("Failure clearing or creating /dStorage");
            System.exit(1);
        }
        new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3])).start();
    }

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        var allContents = directoryToBeDeleted.listFiles();
        var res = true;
        if (allContents != null) {
            for (var file : allContents) {
                res = res && deleteDirectory(file);
            }
        }
        return res && directoryToBeDeleted.delete();
    }

    private void closeConnections() {
        open = false;
        for (var s : dStoreMap.keySet()) {
            try {
                s.close();
            } catch (Exception e) {
                System.err.println("Error closing connections to DStores: " + e);
            }
        }
        for (var s : clients) {
            try {
                s.close();
            } catch (Exception e) {
                System.err.println("Error closing connections to clients: " + e);
            }
        }
        dStoreMap.clear();
        clients.clear();
        System.exit(0);
    }

    private void acceptConnections() {
        new Thread(() -> {
            open = true;
            try {
                var ss = new ServerSocket(cport);
                while (open) {
                    try {
                        final var connection = ss.accept();
                        new Thread(() -> {
                            try {
                                var in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                                String msg = in.readLine();
                                while (msg != null) {
                                    handleMessage(msg, connection);
                                    try {
                                        msg = in.readLine();
                                    } catch (SocketTimeoutException e) {
                                        if (dStoreMap.containsKey(connection)) {
                                            msg = "";
                                            index.removeTimedOutFiles(dStoreMap.get(connection));
                                        }
                                    }
                                }
                                connection.close();
                                in.close();
                                dStoreMap.remove(connection);
                                clients.remove(connection);
                            } catch (Exception e) {
                                System.err.println("Error receiving data from " + connection + ": " + e);
                                e.printStackTrace();
                            }
                        }).start();
                    } catch (Exception e) {
                        System.err.println("Error accepting connection: " + e);
                    }
                }
                ss.close();
            } catch (Exception e) {
                System.err.println("Error creating ServerSocket in Controller: " + e);
            }
        }).start();
    }

    public Set<Integer> getRDStores() {
        var res = new HashSet<Integer>();
        var tmp = new ArrayList<>(dStoreMap.values().stream().toList());
        Collections.shuffle(tmp);
        for (int i = 0; i < R; i++) {
            res.add(tmp.get(i));
        }
        return res;
    }

    public void start() {
        acceptConnections();
    }

    public void sendMessage(String msg, Socket dest) {
        try {
            var out = new PrintWriter(dest.getOutputStream());
            out.println(msg);
            out.flush();
        } catch (IOException e) {
            System.err.println("Error sending message: " + msg + " to dest " + dest);
            e.printStackTrace();
        }
    }

    private void rebalance() {
        Socket dStore;
        BufferedReader in;
        String[] args;
        String msg;
//        var filesTotal = new ArrayList<String[]>();
        var files = new HashSet<String>();
        for (Map.Entry<Socket, Integer> e : dStoreMap.entrySet()) {
            dStore = e.getKey();
            sendMessage("LIST", dStore);
            try {
                in = new BufferedReader(new InputStreamReader(dStore.getInputStream()));
                msg = in.readLine();
                while (msg != null) {
                    args = msg.split(" ");
                    if (msg.startsWith("LIST")) {
                        //filesTotal.add(Arrays.copyOfRange(args, 1, args.length, String[].class));
                        files.addAll(Arrays.asList(args).subList(1, args.length));
                        msg = null;
                    }
                    try {
                        msg = in.readLine();
                    } catch (SocketTimeoutException e1) {

                    }
                }
            } catch (IOException e2) {
                System.err.println("Error performing rebalance: " + e2);
                e2.printStackTrace();
            }
        }
        //String msg;
        DistributedFile f;
        for (String file : files) {
            if (!index.containsFile(file)) {
                f = index.getFile(file);
                for (var s : f.getDStores()) {
                    for (var e : dStoreMap.entrySet()) {
                        if (e.getValue().equals(s)) {
                            try {
                                var in2 = new BufferedReader(new InputStreamReader(e.getKey().getInputStream()));
//                                msg = in.readLine();
//                                while (msg != null) {
//
//                                }
                            } catch (IOException e3) {
                                System.err.println("Error removing ghost files: " + e3);
                                e3.printStackTrace();
                            }
                            sendMessage("REMOVE " + file, e.getKey());
                        }
                    }
                }
                index.removeFile(file);
            }
        }
//        var upper = Math.ceil((R.doubleValue() * files.size()) / dStoreMap.size());
//        var lower = Math.floor((R.doubleValue() * files.size()) / dStoreMap.size());
//        for (String[] dStore : filesTotal) {
//            if (dStore.length < upper && dStore.length > lower) {
//            } else {
//
//            }
//        }
    }

    private void handleMessage(String msg, Socket sender) {
        var args = msg.split(" ");

        // DStore connecting.

        if (msg.startsWith("JOIN")) {
            dStoreMap.put(sender, Integer.parseInt(msg.split(" ")[1]));
            try {
                sender.setSoTimeout(timeout);
            } catch (SocketException e) {
                System.err.println("Error adding timeout to socket: " + e);
                e.printStackTrace();
            }
            System.out.println("DStore " + Integer.parseInt(msg.split(" ")[1]) + " connected");
        }

        // STORE commands.

        else if (msg.startsWith("STORE_ACK")) {
            synchronized (index) {
                var file = index.getFile(args[1]);
                try {
                    file.ackReceived();
                    System.out.println("STORE_ACK received from " + dStoreMap.get(sender) + " for: " + file.getFilename() + ", " + (R - file.getAcksReceived()) + " remaining");
                    if (file.getAcksReceived() >= R) {
                        file.setState(State.STORE_COMPLETE);
                        sendMessage("STORE_COMPLETE", file.getStoredBy());
                        System.out.println("Store complete for " + file.getFilename());
                    }
                } catch (NullPointerException e) {
                    System.out.println("STORE_ACK received from " + dStoreMap.get(sender) + " for deleted file: " + args[1]);
                }
            }
        } else if (msg.startsWith("STORE")) {
            synchronized (index) {
                clients.add(sender);
                if (dStoreMap.size() < R) {
                    sendMessage("ERROR_NOT_ENOUGH_DSTORES", sender);
                } else if (index.containsFile(args[1])) {
                    sendMessage("ERROR_FILE_ALREADY_EXISTS", sender);
                } else {
                    var stores = getRDStores();
                    index.addFile(new DistributedFile(args[1], Integer.parseInt(args[2]), State.STORE_IN_PROGRESS, stores, sender));
                    var reply = new StringBuilder("STORE_TO");
                    for (var n : stores) {
                        reply.append(" ").append(n);
                    }
                    System.out.println("Storing " + args[1] + " to DStores: " + reply);
                    sendMessage(reply.toString(), sender);
                }
            }
        }

        // LOAD commands.

        else if (msg.startsWith("RELOAD")) {
            synchronized (index) {
                var file = index.getFile(args[1]);
                file.reloadAttempted();
                if (file.getReloadAttempts() < file.getDStores().size()) {
                    if (dStoreMap.size() < R) {
                        sendMessage("ERROR_NOT_ENOUGH_DSTORES", sender);
                    } else if (!index.containsFile(args[1]) || file.getState().equals(State.STORE_IN_PROGRESS) || file.getState().equals(State.REMOVE_IN_PROGRESS)) {
                        sendMessage("ERROR_FILE_DOES_NOT_EXIST", sender);
                    } else {
                        System.out.println("Re-loading " + file.getFilename() + " from DStore: " + file.getDStores().toArray()[file.getReloadAttempts()]);
                        sendMessage("LOAD_FROM " + file.getDStores().toArray()[file.getReloadAttempts()] + " " + file.getSize(), sender);
                    }
                } else {
                    System.out.println("Couldn't load: " + file.getFilename());
                    sendMessage("ERROR_LOAD", sender);
                }
            }
        } else if (msg.startsWith("LOAD")) {
            synchronized (index) {
                var file = index.getFile(args[1]);
                file.resetReloads();
                if (dStoreMap.size() < R) {
                    sendMessage("ERROR_NOT_ENOUGH_DSTORES", sender);
                } else if (!index.containsFile(args[1]) || file.getState().equals(State.STORE_IN_PROGRESS) || file.getState().equals(State.REMOVE_IN_PROGRESS)) {
                    sendMessage("ERROR_FILE_DOES_NOT_EXIST", sender);
                } else {
                    System.out.println("Loading " + file.getFilename() + " from DStore: " + file.getDStores().toArray()[file.getReloadAttempts()]);
                    sendMessage("LOAD_FROM " + file.getDStores().toArray()[file.getReloadAttempts()] + " " + file.getSize(), sender);
                }
            }
        }

        // Remove command.

        else if (msg.startsWith("REMOVE_ACK")) {
            synchronized (index) {
                var file = index.getFile(args[1]);
                try {
                    file.ackReceived();
                    System.out.println("REMOVE_ACK received from " + dStoreMap.get(sender) + " for: " + file.getFilename() + ", " + (R - file.getAcksReceived()) + " remaining");
                    if (file.getAcksReceived() >= R) {
                        file.setState(State.REMOVE_COMPLETE);
                        sendMessage("REMOVE_COMPLETE", file.getRemovedBy());
                        index.removeFile(file);
                        System.out.println("Remove complete for " + file.getFilename());
                    }
                } catch (NullPointerException e) {
                    System.out.println("REMOVE_ACK received from " + dStoreMap.get(sender) + " for deleted file: " + args[1]);
                }
            }
        } else if (msg.startsWith("REMOVE")) {
            synchronized (index) {
                var file = index.getFile(args[1]);
                if (dStoreMap.size() < R) {
                    sendMessage("ERROR_NOT_ENOUGH_DSTORES", sender);
                } else if (!index.containsFile(args[1]) || file.getState().equals(State.STORE_IN_PROGRESS) || file.getState().equals(State.REMOVE_IN_PROGRESS)) {
                    sendMessage("ERROR_FILE_DOES_NOT_EXIST", sender);
                } else {
                    file.setState(State.REMOVE_IN_PROGRESS);
                    file.resetAcks();
                    file.setRemovedBy(sender);
                    for (Integer s : file.getDStores()) {
                        for (Map.Entry<Socket, Integer> e : dStoreMap.entrySet()) {
                            if (e.getValue().equals(s)) {
                                sendMessage("REMOVE " + file.getFilename(), e.getKey());
                            }
                        }
                    }
                    System.out.println("Removing " + args[1] + " from DStores: " + file.getDStores().toString());
                }
            }
        }

        // LIST command.

        else if (msg.startsWith("LIST")) {
            if (dStoreMap.size() < R) {
                sendMessage("ERROR_NOT_ENOUGH_DSTORES", sender);
            } else {
                System.out.println("Listing files in the index.");
                synchronized (index) {
                    sendMessage("LIST" + index.getAvailableFiles(), sender);
                }
            }
        }
    }
}

class Index {

    private final Set<DistributedFile> files;

    public Index() {
        files = new LinkedHashSet<>();
    }

    public boolean containsFile(String filename) {
        return getFile(filename) != null;
    }

    public DistributedFile getFile(String filename) {
        for (var f : files) {
            if (f.getFilename().equals(filename)) return f;
        }
        return null;
    }

    public String getAvailableFiles() {
        StringBuilder res = new StringBuilder();
        for (DistributedFile f : files) {
            if (!f.getState().equals(State.STORE_IN_PROGRESS) && !f.getState().equals(State.REMOVE_IN_PROGRESS)) {
                res.append(" ").append(f.getFilename());
            }
        }
        return res.toString();
    }

    public synchronized void removeTimedOutFiles(int dStorePort) {
        for (DistributedFile f : files) {
            if (f.getDStores().contains(dStorePort) && (f.getState().equals(State.STORE_IN_PROGRESS) || f.getState().equals(State.REMOVE_IN_PROGRESS))) {
                files.remove(f);
                System.out.println("Removed file " + f.getFilename() + " for: " + dStorePort);
            }
        }
        //files.removeIf(f -> f.getState().equals(State.STORE_IN_PROGRESS) && f.getDStores().contains(dStorePort));
    }

    public void addFile(DistributedFile f) {
        files.add(f);
    }

    public void removeFile(DistributedFile f) {
        files.remove(f);
    }

    public void removeFile(String f) {
        files.remove(getFile(f));
    }
}

class DistributedFile {

    private final String filename;
    private final int size;
    private final Set<Integer> dStores;
    private final Socket storedBy;
    private Socket removedBy;
    private State state;
    private int acksReceived;
    private int reloadAttempts;

    public DistributedFile(String filename, int size, State state, Set<Integer> dStores, Socket storedBy) {
        this.filename = filename;
        this.size = size;
        this.state = state;
        this.dStores = dStores;
        this.storedBy = storedBy;
        acksReceived = 0;
        reloadAttempts = 0;
    }

    public void resetReloads() {
        reloadAttempts = 0;
    }

    public void resetAcks() {
        acksReceived = 0;
    }

    public Socket getRemovedBy() {
        return removedBy;
    }

    public void setRemovedBy(Socket s) {
        removedBy = s;
    }

    public int getSize() {
        return size;
    }

    public Set<Integer> getDStores() {
        return dStores;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public Socket getStoredBy() {
        return storedBy;
    }

    public int getAcksReceived() {
        return acksReceived;
    }

    public int getReloadAttempts() {
        return reloadAttempts;
    }

    public void reloadAttempted() {
        reloadAttempts++;
    }

    public void ackReceived() {
        acksReceived++;
    }

    public String getFilename() {
        return filename;
    }
}