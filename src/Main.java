import java.io.File;
import java.util.Objects;

public class Main {

    public static void main(String[] args) {
        new Thread(() -> {
            Controller controller = new Controller(4322, 4, 6, 7);
            controller.start();
        }).start();
        try {
            Thread.sleep(1000);
        } catch (Exception ignored) {
        }
        File dir = new File(System.getProperty("user.dir") + "/dStorage");
        boolean res = true;
        if (dir.exists()) {
            for (File f : Objects.requireNonNull(dir.listFiles())) {
                res = res && f.delete();
            }
        } else {
            res = dir.mkdir();
        }
        if (res) {
            for (int i = 1; i < 5; i++) {
                int finalI = i;
                new Thread(() -> {
                    String file = "d" + finalI;
                    DStore dStore = new DStore(1322 + finalI, 4322, 7, file);
                    dStore.start();
                }).start();
            }
        }
        System.out.println("Threads started.");
    }
}
