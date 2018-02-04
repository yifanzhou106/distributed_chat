package project1;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import project1.ChatProto.Chat;
import project1.ChatProto.Reply;
import project1.ChatProto.ZKData;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * A chat client that uses raw sockets to communicate with clients
 *
 * @Author Yifan Zhou
 */
public class chatClientTest {
    public static final int PORT = 6000;
    static volatile boolean isShutdown = false;

    public static final int ZpPORT = 9090;
    public static final String ZpHOST = "localhost";

    public static final String group = "/CS682_Chat";
    public static final String member = "/yifanzhoutest3";

    final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    final ExecutorService threads = Executors.newFixedThreadPool(4);

    private Map<String, ArrayList<String>> userMap = new HashMap();

    /**
     * Main function load hotelData and reviews, Then call startServer.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        chatClientTest client = new chatClientTest();
        client.beginChat();

    }

    /**
     * create a welcoming socket
     * Code from 601
     */
    public void beginChat() {


        try {
            ServerSocket welcomingSocket = new ServerSocket(PORT);
            //   System.out.println("Waiting for clients to connect...");
            ZooKeeper zk = connectZooKeeper(); //Create zookeeper instance
            joinZooKeeper(zk);
            threads.submit(new userInput(zk)); //Create UI thread
            while (!isShutdown) {
                System.out.println("1111");
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


    public class userInput implements Runnable {
        ZooKeeper zk;

        private userInput(ZooKeeper zk) {
            this.zk = zk;
        }

        @Override
        public void run() {
            while (!isShutdown) {
                boolean ifPrint = false;
                listZooKeeperMember(zk, userMap, ifPrint);

                Scanner reader = new Scanner(System.in);
                System.out.println("Enter your choices (Enter \"help\" for help): ");
                String userChoice = reader.nextLine();
                String[] splitedUserChoice = userChoice.split(" ");


                switch (splitedUserChoice[0]) {
                    case "help":
                        System.out.println("\n**************************");
                        System.out.println("\n1. send $name ");
                        System.out.println("\n2. bcast $message");
                        System.out.println("\n3. list");
                        System.out.println("\n4. history\n");
                        System.out.println("\n**************************");
                        break;

                    case "send":
                        if (!splitedUserChoice[1].isEmpty()) {
                            String name = splitedUserChoice[1];
                            System.out.println("Enter your message: ");
                            String message = reader.nextLine();
                            threads.submit(new SendMessageWorker(name, message));
                        } else {
                            System.out.println("Wrong data format");
                        }
                        break;

                    case "bcast":
                        boolean isBcast = true;
                        System.out.println("Enter your message: ");
                        String message = reader.nextLine();
                        threads.submit(new SendMessageWorker(message, isBcast));
                        break;

                    case "list":
                        ifPrint = true;
                        listZooKeeperMember(zk, userMap, ifPrint);
                        break;

                    case "history":
                        break;


                }
            }

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
                    if (event.getState() == Event.KeeperState.SyncConnected) {
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
        ZKData data = ZKData.newBuilder().setIp("127.0.0.1").setPort("6000").build();
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
    private void listZooKeeperMember(ZooKeeper zk, Map<String, ArrayList<String>> userMap, boolean ifPrint) {
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

                    ArrayList<String> userData = new ArrayList();
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
        }

        rwl.writeLock().unlock();
    }


    private class SendMessageWorker implements Runnable {
        private Socket connectionSocket = new Socket();
        final static String EOT = "EOT";
        private String name = null;
        private String message = null;
        private boolean isBcast = false;
        private ArrayList<String> userData = new ArrayList();
        private String sip;
        private String sport;

        private SendMessageWorker(String name, String message) {
            this.message = message;
            this.name = name;
        }

        private SendMessageWorker(String message, boolean isBcast) {
            this.message = message;
            this.isBcast = isBcast;
        }

        @Override
        public void run() {
            System.out.println("Your message is: " + message);
            try {
                //get ip and port from user map
                userData = userMap.get(name);
                sip = userData.get(0);
                sport = userData.get(1);
                InetAddress ip = InetAddress.getByName(sip);
                int port = Integer.parseInt(sport);
                //Create connection
                connectionSocket = new Socket(ip, port);

                Chat sendMessage = Chat.newBuilder().setMessage(message).setFrom(member).setIsBcast(isBcast).build();
                OutputStream outstream = connectionSocket.getOutputStream();
                sendMessage.writeDelimitedTo(outstream);

                InputStream instream = connectionSocket.getInputStream();
                Reply replyMessage = Reply.getDefaultInstance();
                replyMessage = replyMessage.parseDelimitedFrom(instream);
                System.out.println(replyMessage.getStatus() + " "+replyMessage.getMessage());


            } catch (IOException e) {
                System.out.println(e);

            }
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
            try  {
                InputStream instream = connectionSocket.getInputStream();
                Chat receiveMessage = Chat.getDefaultInstance();
                receiveMessage = receiveMessage.parseDelimitedFrom(instream);
                if (!receiveMessage.getIsBcast())
                    System.out.println(receiveMessage.getFrom()+" says: " + receiveMessage.getMessage());
                else
                {
                    System.out.println(receiveMessage.getFrom()+" broadcast: " + receiveMessage.getMessage());
                }
                Reply responseMessage = Reply.newBuilder().setStatus(200).setMessage("Ok").build();
                OutputStream outstream = connectionSocket.getOutputStream();
                responseMessage.writeDelimitedTo(outstream);

            } catch (IOException e) {
                System.out.println(e);

            }
        }

    }
}