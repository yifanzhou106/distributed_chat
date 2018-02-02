package project1;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import project1.ChatProto.ZKData;
import project1.ChatProto.Chat;
import project1.ChatProto.Reply;


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

    public static final String group = "/zkdemo";
    public static final String member = "/yifanzhou";

    final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    final ExecutorService threads = Executors.newFixedThreadPool(4);

    private Map userMap = new HashMap();

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

    public void userInput() {
        while (!isShutdown) {

        }
    }

    /**
     * Create zookeeper instance
     * Codes from zp example
     *
     * @return
     */
    private ZooKeeper connectZooKeeper() {

        ZooKeeper zk = null;
        try {
            //Connect to ZK instance
            final CountDownLatch connectedSignal = new CountDownLatch(1);
            zk = new ZooKeeper(ZpHOST + ":" + ZpPORT, 1000, new Watcher() {
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
        } catch (IOException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
        return zk;
    }


    /**
     * Join zookeeper
     * Codes from zp example
     *
     * @return
     */
    private void joinZooKeeper(ZooKeeper zk) {
        ZKData data = ZKData.newBuilder().setIp("127.0.0.1").setPort("7000").build();
        try {
            String createdPath = zk.create(group + member,
                    data.toByteArray(),  //probably should be something more interesting here...
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);//_SEQUENTIAL
            System.out.println("Joined group " + group + member);

        } catch (KeeperException ke) {
            System.out.println("Unable to or have already joined group " + group + " as " + member);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }


    /**
     * List zookeeper members
     * Codes from zp example
     *
     * @return
     */
    private void listZooKeeperMember(ZooKeeper zk, HashMap userMap) {

        rwl.writeLock().lock();
        try {
            List<String> children = zk.getChildren(group, false);
            for (String child : children) {
                System.out.println(child);

                //get data about each child
                Stat s = new Stat();
                byte[] raw = zk.getData(group + "/" + child, false, s);
                ZKData data = ZKData.parseFrom(raw);
                if (data != null) {
                    String ip = data.getIp();
                    String port = data.getPort();

                    ArrayList userData = new ArrayList();
                    userData.add(ip);
                    userData.add(port);
                    System.out.print("IP: " + ip + "\tPort: " + port);
                    userMap.put(child, userData);
                    System.out.print("\n");

                } else {
                    System.out.println("\tNO DATA");
                }
            }
        } catch (KeeperException ke) {
            System.out.println("Unable to list members of group " + group);
        } catch (InvalidProtocolBufferException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        } finally {
            rwl.writeLock().unlock();
        }


    }

    /**
     * create a welcoming socket to
     * listen for client connections; once a new client request comes in, then
     * create a connection socket for this client and add a new Runnable task to the ExecutorService
     * Code from 601
     */
    public void receiveMessage() {


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

                while (line != null && !line.trim().equals(EOT)) {
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