import java.io.File;

public class Main {

    public static void main(String[] args) {
        var timeout = 5000;
        var cport = 4322;
        var dStorePorts = 1300;
        var rebalancePeriod = 7;
        var R = 2;
        new Thread(() -> {
            var dir = new File(System.getProperty("user.dir") + "/dStorage");
            if (dir.exists()) {
                Controller.deleteDirectory(dir);
            }
            if (!dir.mkdir()) {
                System.err.println("Failure clearing or creating /dStorage");
                System.exit(1);
            }
            Controller controller = new Controller(cport, R, timeout, rebalancePeriod);
            controller.start();
        }).start();
        try {
            Thread.sleep(1000);
        } catch (Exception ignored) {
        }
        for (int i = 0; i < 5; i++) {
            int finalI = i;
            new Thread(() -> {
                String file = "d" + finalI;
                DStore dStore = new DStore(dStorePorts + finalI, cport, timeout, file);
                dStore.start();
            }).start();
        }
        System.out.println("Threads started.");
        try {
            Thread.sleep(1000);
        } catch (Exception ignored) {
        }
        File uploadFolder = new File("src/to_store");
        File[] fileList = uploadFolder.listFiles();
        var client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
        try {
            client.connect();
            assert fileList != null;
            client.store(fileList[0]);
            client.disconnect();
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }
    }
}
