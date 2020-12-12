import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ClusterHealer implements Watcher {
    //variable for worker nodes constant
    private static final String WORKER_ZNODE = "/worker_";
    //path to the program
    private static final String PATH_TO_PROGRAM = "./";
    //Initialising Zookeeper
    private static ZooKeeper zooKeeper;
    //Setting the address for zookeeper
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    //timeout in seconds
    private static final int SESSION_TIMEOUT = 3000;
    //parent node constant
    private static final String WORKERS_PARENT_ZNODE = "/workers";

    // Path to the worker jar
    private final String pathToProgram;
    // The number of worker instances we need to maintain at all times
    private final int numberOfWorkers;
    //constructor
    public ClusterHealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;

    }//ClusterHealer

    /**
     * Check if the `/workers` parent znode exists, and create it if it doesn't. Decide for yourself what type of znode
     * it should be (e.g.persistent, ephemeral etc.). Check if workers need to be launched.
     */
    public void initialiseCluster() throws KeeperException, InterruptedException {
        //check if the parent node is created
        Stat stat = zooKeeper.exists(WORKERS_PARENT_ZNODE, this);
        //if empty
        if (stat == null) {
            //creating a worker that is persistent between sessions
            zooKeeper.create(WORKERS_PARENT_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        // call method
      checkRunningWorkers();

    }//initialiseCluster

    /**
     * Instantiates a Zookeeper client, creating a connection to the Zookeeper server.
     */
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }//connectToZookeeper

    /**
     * Keeps the application running waiting for Zookeeper events.
     */
    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }//run

    /**
     * Closes the Zookeeper client connection.
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }//close

    /**
     * Handles Zookeeper events related to: - Connecting and disconnecting from the Zookeeper server. - Changes in the
     * number of workers currently running.
     *
     * @param event A Zookeeper event
     */
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            /*
            Switch cases on each type of event
            None: if zookeeper connects successfully print a success message
            and print a disconnected message when disconnected.

            NodeChildrenChanged: if a child is removed print a message and call check workers
             */
            case None:
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");

                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeChildrenChanged:

                System.out.println("Child : " + WORKER_ZNODE + " Deleted");
                checkRunningWorkers();
                break;
        }

    }//process

    /**
     * Checks how many workers are currently running.
     * If less than the required number, then start a new worker.
     */
    public void checkRunningWorkers() {
        /*
        putting a the parent znode and instance of watcher into a list called children
        if the number of children is less than the entered amount of children call start worker
         */

        try {
            List<String> children = zooKeeper.getChildren(WORKERS_PARENT_ZNODE, this);

            System.out.println("There are currently: " + children.size() + " workers");
            if (children.size() < numberOfWorkers) {
                startWorker();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }


    }//checkRunningWorkers

    /**
     * Starts a new worker using the path provided as a command line parameter.
     *
     * @throws IOException
     */
    public void startWorker() throws IOException {
        File file = new File(pathToProgram);
        String command = "java -jar " + file.getName();
        System.out.println(String.format("Launching worker instance : %s ", command));
        Runtime.getRuntime().exec(command, null, file.getParentFile());

    }//startWorker

}//ClusterHealer
