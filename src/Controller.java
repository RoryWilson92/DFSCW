import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;

enum State {
    STORE_IN_PROGRESS, STORE_COMPLETE
}

public class Controller {

    private final int R;
    private final int timeout;
    private final int rebalancePeriod;
    private final int cport;
    private final Set<Socket> dStores;
    private final List<Integer> dStorePorts;
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
        dStorePorts = new ArrayList<>();
        dStores = new HashSet<>();
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

    private static boolean deleteDirectory(File directoryToBeDeleted) {
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
        for (var s : dStores) {
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
        dStores.clear();
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
                                String msg;
                                while ((msg = in.readLine()) != null) handleMessage(msg, connection);
                                connection.close();
                                dStores.remove(connection);
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
                System.err.println("Error creating ServerSocket: " + e);
            }
        }).start();
    }

    public Set<Integer> getRDStores() {
        var res = new HashSet<Integer>();
        Collections.shuffle(dStorePorts);
        for (var i = 0; i < R; i++) {
            res.add(dStorePorts.get(i));
        }
        return res;
    }

    public void start() {
        acceptConnections();
//        try {
//            Thread.sleep(20000);
//        } catch (Exception ignored) {
//        }
        //closeConnections();
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

    private void handleMessage(String msg, Socket sender) {
        var args = msg.split(" ");
        if (msg.startsWith("DSTORE")) {
            dStorePorts.add(Integer.parseInt(msg.split(" ")[1]));
            try {
                sender.setSoTimeout(timeout);
            } catch (SocketException e) {
                System.err.println("Error adding timeout to socket: " + e);
                e.printStackTrace();
            }
            dStores.add(sender);
        } else if (msg.startsWith("STORE_ACK")) {
            //TODO implement timeout
            synchronized (index) {
                index.ackReceived(args[1]);
                if (index.getFile(args[1]).getAcksReceived() == R) {
                    index.setState(args[1], State.STORE_COMPLETE);
                    sendMessage("STORE_COMPLETE", index.getStoredBy(args[1]));
                }
            }
        } else if (msg.startsWith("STORE")) {
            synchronized (index) {
                clients.add(sender);
                if (dStores.size() < R) {
                    sendMessage("ERROR_NOT_ENOUGH_DSTORES", sender);
                } else if (index.containsFile(args[1])) {
                    sendMessage("ERROR_FILE_ALREADY_EXISTS", sender);
                } else {
                    var stores = getRDStores();
                    var file = new DistributedFile(args[1], Integer.parseInt(args[2]), State.STORE_IN_PROGRESS, stores, sender);
                    var timer = new Timer(timeout, () -> index.removeFile(file));
                    index.addFile(file);
                    index.addTimer(timer);
                    var reply = new StringBuilder("STORE_TO");
                    for (var n : stores) {
                        reply.append(" ").append(n);
                    }
                    sendMessage(reply.toString(), sender);
                    timer.start();
                }
            }
        }
    }
}

class Index {

    private final Set<DistributedFile> files;
    private final Set<Timer> timers;
    private final Map<DistributedFile, Timer> files2;

    public Index() {
        files = new HashSet<>();
        timers = new HashSet<>();
        files2 = new HashMap<>();
    }

    public boolean containsFile(String filename) {
        return getFile(filename) != null;
    }

    public Socket getStoredBy(String filename) {
        return getFile(filename).getStoredBy();
    }

    public DistributedFile getFile(String filename) {
        for (var f : files) {
            if (f.getFilename().equals(filename)) return f;
        }
//        for (Map.Entry<DistributedFile, Timer> p: files2) {
//
//        }
        return null;
    }

    public void ackReceived(String filename) {
        Objects.requireNonNull(getFile(filename)).ackReceived();
    }

    public void setState(String filename, State state) {
        Objects.requireNonNull(getFile(filename)).setState(state);
    }

    public void addTimer(Timer timer) {
        timers.add(timer);
    }

    public void removeTimer(Timer timer) {
        timers.remove(timer);
    }

    public void addFile(DistributedFile f) {
        files.add(f);
    }

    public void removeFile(DistributedFile f) {
        files.remove(f);
    }
}

class Timer {

    private final int duration;
    private final Runnable callback;
    private long startTime;
    private Thread timerThread;
    private boolean run;

    public Timer(int millis, Runnable callback) {
        duration = millis;
        this.callback = callback;
        run = true;
    }

    public void start() {
        timerThread = new Thread(() -> {
            while (run) {
                startTime = System.currentTimeMillis();
                while (startTime + duration < System.currentTimeMillis()) {
                }
                run = false;
                callback.run();
            }
        });
        timerThread.start();
    }

    public void cancel() {
        run = false;
    }
}

class DistributedFile {

    private final String filename;
    private final int size;
    private final Set<Integer> dStores;
    private final Socket storedBy;
    private State state;
    private int acksReceived;
    private Timer timer;

    public DistributedFile(String filename, int size, State state, Set<Integer> dStores, Socket storedBy) {
        this.filename = filename;
        this.size = size;
        this.state = state;
        this.dStores = dStores;
        this.storedBy = storedBy;
        acksReceived = 0;
    }

    public Socket getStoredBy() {
        return storedBy;
    }

    public int getAcksReceived() {
        return acksReceived;
    }

    public void ackReceived() {
        acksReceived++;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getFilename() {
        return filename;
    }
}