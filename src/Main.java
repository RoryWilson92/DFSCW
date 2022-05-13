import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class Main {

    public static Integer t = 0;

    public static void main(String[] args) {
        var timeout = 5000;
        var cport = 4322;
        var dStorePorts = 1300;
        var rebalancePeriod = 7;
        var R = 4;
        new Thread(() -> {
            var dir = new File(System.getProperty("user.dir") + "/dStorage");
            if (dir.exists()) {
                Controller.deleteDirectory(dir);
            }
            if (!dir.mkdir()) {
                System.err.println("Failure clearing or creating /dStorage");
                System.exit(1);
            }
            var controller = new Controller(cport, R, timeout, rebalancePeriod);
            controller.start();
        }).start();
        try {
            Thread.sleep(1000);
        } catch (Exception ignored) {
        }
        for (int i = 0; i < 50; i++) {
            int finalI = i;
            new Thread(() -> {
                var file = "d" + finalI;
                var dStore = new DStore(dStorePorts + finalI, cport, timeout, file);
                dStore.start();
            }).start();
        }
        System.out.println("Threads started.");
        try {
            Thread.sleep(1000);
        } catch (Exception ignored) {
        }
        var downloadFolder = new File("src/downloads");
        if (downloadFolder.exists()) {
            Controller.deleteDirectory(downloadFolder);
        }
        if (!downloadFolder.mkdir()) {
            System.err.println("Failure clearing or creating /downloads");
            System.exit(1);
        }
        var uploadFolder = new File("src/to_store");
        var fileList = uploadFolder.listFiles();
        Integer[] arr = new Integer[10];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = i;
        }
        Collections.shuffle(Arrays.asList(arr));
        System.out.println(Arrays.toString(arr));
        var r = new Random();
        for (int i = 0; i < 6; i++) {
            int finalI = i;
            var random = r.nextInt(0, 10);
            new Thread(() -> {
                var client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
                try {
                    client.connect();
                    assert fileList != null;
                    client.store(fileList[random]);
                    client.list();
                    client.load(fileList[random].getName(), downloadFolder);
                    //client.remove(fileList[random].getName());
                    client.disconnect();
                    synchronized (t) {
                        t++;
                    }
                } catch (Exception e) {
                    System.err.println(e);
                    e.printStackTrace();
                }
            }).start();
        }
        while (t < 6) {
        }
        System.exit(0);
    }
}
