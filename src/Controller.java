import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.stream.Collectors;

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
                                //in.close();
                                //connection.close();
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
                //ss.close();
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
//        new Thread(() -> {
//            while (true) {
//                try {
//                    Thread.sleep(rebalancePeriod);
//                    rebalance();
//                } catch (InterruptedException e) {
//                    System.err.println("Rebalance sleep interrupted.");
//                }
//            }
//        }).start();
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

    // Rebalance not used as DStore side not complete.

    private void rebalance() {
        synchronized (index) {
            try {
                System.out.println("Rebalance started.");
                BufferedReader in;
                String[] args;
                var files = new HashSet<String>();
                var oldIndex = new HashMap<Integer, List<String>>();
                for (var e : dStoreMap.entrySet()) {
                    var dStore = e.getKey();
                    sendMessage("LIST", dStore);
                    System.out.println("Sent LIST request to DStore: " + dStoreMap.get(dStore));
                    try {
                        in = new BufferedReader(new InputStreamReader(dStore.getInputStream()));
                        var msg = in.readLine();
                        //in.close();
                        args = msg.split(" ");
                        if (msg.startsWith("LIST")) {
                            System.out.println("LIST received from DStore: " + dStoreMap.get(dStore));
                            oldIndex.put(e.getValue(), new ArrayList<>(Arrays.asList(args).subList(1, args.length)));
                            files.addAll(Arrays.asList(args).subList(1, args.length));
                        }
                    } catch (IOException e2) {
                        System.err.println("Error performing rebalance: " + e2);
                        e2.printStackTrace();
                    }
                }
                var newIndexTmp = new HashMap<>(oldIndex.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue()))));
                for (var file : files) {
                    int count = R;
                    for (var dStore : newIndexTmp.entrySet()) {
                        if (dStore.getValue().contains(file)) {
                            count--;
                        }
                    }
                    for (int i = 0; i < count; i++) {
                        for (var dStore : newIndexTmp.entrySet()) {
                            if (!dStore.getValue().contains(file)) {
                                dStore.getValue().add(file);
                                break;
                            }
                        }
                    }
                }
                var newIndex = new ArrayList<>(newIndexTmp.entrySet().stream().toList());
                newIndex.sort((o1, o2) -> Integer.compare(o2.getValue().size(), o1.getValue().size()));
                try {
                    while (newIndex.get(0).getValue().size() - newIndex.get(newIndex.size() - 1).getValue().size() > 1){
                        for (var s : newIndex.get(0).getValue()) {
                            if (!newIndex.get(newIndex.size() - 1).getValue().contains(s)) {
                                newIndex.get(newIndex.size() - 1).getValue().add(s);
                                newIndex.get(0).getValue().remove(s);
                                break;
                            }
                        }
                        newIndex.sort((o1, o2) -> Integer.compare(o2.getValue().size(), o1.getValue().size()));
                    }
                } catch (Exception e) {
                    System.err.println(e);
                }
                var removesList = new HashMap<Integer, Set<String>>();
                var wantedList = new HashMap<Integer, Set<String>>();
                for (var newI : newIndex) {
                    var newSet = new HashSet<>(newI.getValue());
                    var oldSet = new HashSet<>(oldIndex.get(newI.getKey()));
                    var removes = new HashSet<>(oldSet);
                    removes.removeAll(newSet);
                    removesList.put(newI.getKey(), removes);
                    var moves = new HashSet<>(newSet);
                    moves.removeAll(oldSet);
                    wantedList.put(newI.getKey(), moves);
                }
                var movesList = new HashMap<Integer, HashMap<String, Set<Integer>>>();
                for (var source : oldIndex.entrySet()) {
                    var tmp2 = new HashMap<String, Set<Integer>>();
                    for (var file : source.getValue()) {
                        var tmp = new HashSet<Integer>();
                        for (var dstore : wantedList.entrySet()) {
                            if (dstore.getValue().contains(file)) {
                                tmp.add(dstore.getKey());
                            }
                        }
                        tmp2.put(file, tmp);
                    }
                    movesList.put(source.getKey(), tmp2);
                }
                for (var dstore : dStoreMap.entrySet()) {
                    StringBuilder msg = new StringBuilder("REBALANCE");
                    try {
                        msg.append(" ").append(movesList.get(dstore.getValue()).size());
                        for (var send : movesList.get(dstore.getValue()).entrySet()) {
                            msg.append(" ").append(send.getKey()).append(" ").append(send.getValue().size());
                            for (var t : send.getValue()) {
                                msg.append(" ").append(t);
                            }
                        }
                    } catch (NullPointerException ignored) {
                    } catch (Exception e) {
                        System.err.println(e);
                    }
                    try {
                        msg.append(" ").append(removesList.get(dstore.getValue()).size());
                        for (var remove : removesList.get(dstore.getValue())) {
                            msg.append(" ").append(remove);
                        }
                    } catch (NullPointerException ignored) {
                    } catch (Exception e) {
                        System.err.println(e);
                    }
                    sendMessage(msg.toString(), dstore.getKey());
                    System.out.println("Sent REBALANCE message to: " + dstore.getValue());
                    try {
                        in = new BufferedReader(new InputStreamReader(dstore.getKey().getInputStream()));
                        var line = in.readLine();
                        //in.close();
                        if (line.equals("REBALANCE_COMPLETE")) {
                            System.out.println("Rebalance operation completed at DStore: " + dstore.getValue());
                        }
                    } catch (SocketTimeoutException ste) {
                        System.err.println("Rebalance operation not completed at DStore " + dstore.getValue() + " before timeout");
                    } catch (IOException e2) {
                        System.err.println("Error receiving rebalance complete: " + e2);
                        e2.printStackTrace();
                    }
                }
            } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace();
            }
        }
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
            //rebalance();
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