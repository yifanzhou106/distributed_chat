import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * A chat client that uses raw sockets to communicate with clients
 *
 * @Author Yifan Zhou
 */
public class chatClient {
    public static final int PORT = 7000;
    static volatile boolean isShutdown = false;

    public static final int ZpPORT = 9000;
    public static final String ZpHOST = "localhost";

    /**
     * Main function load hotelData and reviews, Then call startServer.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        chatClient client = new chatClient();
        ZooKeeper zk = client.connectZooKeeper();
        client.receiveMessage();
        client.userInput();
    }

    public void userInput()
    {
        while (!isShutdown) {

        }
    }

    /**
     * Create zookeeper instance
     * Codes from zp example
     *
     * @return
     */
    private ZooKeeper connectZooKeeper()
    {
        String group = "/zkdemo";
        String member = "/member";
        String data = "placeholder";
        ZooKeeper zk = null;
    try {
        //Connect to ZK instance
        final CountDownLatch connectedSignal = new CountDownLatch(1);
         zk = new ZooKeeper(ZpHOST + ":" + PORT, 1000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        System.out.println("Connecting...");
        connectedSignal.await();
        System.out.println("Connected");
    }
    catch (IOException e)
    {
        System.out.println(e);
    }
    catch (InterruptedException e)
    {
        System.out.println(e);
    }
        return zk;
    }

    

    /**
     * create a welcoming socket to
     * listen for client connections; once a new client request comes in, then
     * create a connection socket for this client and add a new Runnable task to the ExecutorService
     * Code from 601
     */
    public void receiveMessage() {
        final ExecutorService threads = Executors.newFixedThreadPool(4);

        try {
            ServerSocket welcomingSocket = new ServerSocket(PORT);
            System.out.println("Waiting for clients to connect...");
            while (!isShutdown) {
                Socket clientSocket = welcomingSocket.accept();
                threads.submit(new Worker(clientSocket));
            }
            if (isShutdown) {
                welcomingSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Unable to process client request");
            e.printStackTrace();
        }


    }

    private class Worker implements Runnable {
        private final Socket connectionSocket;
        final static String EOT = "EOT";

        private Worker(Socket connectionSocket) {
            this.connectionSocket = connectionSocket;
        }

        @Override
        public void run() {
            System.out.println("A client connected.");
            try (BufferedReader br = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()))) {
                String message = "";
                String line = br.readLine();

                while(line != null && !line.trim().equals(EOT)) {
                    message += line + "\n";
                    line = br.readLine();
                }
                System.out.println("Client says: " + message);


                PrintWriter pw = new PrintWriter(connectionSocket.getOutputStream());

                String[] requestParam = line.split(" ");
                String method = requestParam[0]; //GET or Post
                String path = requestParam[1]; ///hotelInfo?hotelId=25622


            } catch (IOException e) {
                System.out.println(e);

            }
        }

        }
}