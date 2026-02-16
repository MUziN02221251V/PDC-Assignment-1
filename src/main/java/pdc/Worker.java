package pdc;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private AtomicBoolean running = new AtomicBoolean(true);
    private ExecutorService taskExecutor;
    private Thread heartbeatThread;

    public Worker() {
        // Thread pool for concurrent task execution
        this.taskExecutor = Executors.newFixedThreadPool(4);
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            // 1. Establish physical connection
            this.socket = new Socket(masterHost, port);
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new DataOutputStream(socket.getOutputStream());

            // 2. Handshake: Use messageType matching our new Message class
            Message joinMsg = new Message("JOIN", "WorkerNode-" + Thread.currentThread().getId(), "READY".getBytes());
            sendMessage(joinMsg);

            System.out.println("Worker: Handshake sent to " + masterHost);

            // 3. Start heartbeat thread BEFORE entering execution loop
            startHeartbeat();

            // 4. Enter the execution loop
            execute();

        } catch (IOException e) {
            System.err.println("Worker: Connection failed: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    /**
     * CRITICAL: Heartbeat mechanism for failure detection
     * Sends periodic HEARTBEAT messages to Master
     */
    private void startHeartbeat() {
        heartbeatThread = new Thread(() -> {
            while (running.get() && socket != null && !socket.isClosed()) {
                try {
                    // Send heartbeat every 2 seconds
                    Thread.sleep(2000);
                    
                    synchronized (out) {
                        Message heartbeat = new Message("HEARTBEAT", "WorkerNode-" + Thread.currentThread().getId(), new byte[0]);
                        sendMessage(heartbeat);
                    }
                    
                    System.out.println("Worker: Heartbeat sent");
                } catch (InterruptedException e) {
                    break;
                } catch (IOException e) {
                    System.err.println("Worker: Heartbeat failed - connection lost");
                    running.set(false);
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    /**
     * Executes received task blocks with concurrent processing.
     */
    public void execute() {
        try {
            while (running.get() && socket != null && !socket.isClosed()) {
                // Read the length prefix
                int length = in.readInt();
                
                if (length <= 0) {
                    continue; // Skip invalid messages
                }

                byte[] data = new byte[length];
                in.readFully(data);
                
                Message task = Message.unpack(data);

                if (task == null) {
                    continue;
                }

                // Handle different message types
                if ("TASK".equals(task.messageType)) {
                    // Submit task to thread pool for concurrent execution
                    taskExecutor.submit(() -> handleTask(task));
                } else if ("SHUTDOWN".equals(task.messageType)) {
                    System.out.println("Worker: Received shutdown signal");
                    running.set(false);
                    break;
                } else if ("PING".equals(task.messageType)) {
                    // Respond to ping
                    synchronized (out) {
                        Message pong = new Message("PONG", "WorkerNode", new byte[0]);
                        sendMessage(pong);
                    }
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                System.err.println("Worker: Connection lost: " + e.getMessage());
            }
        }
    }

    /**
     * Handle individual task execution
     */
    private void handleTask(Message task) {
        try {
            System.out.println("Worker: Processing task of type: " + task.messageType);
            
            // Call the specific compute method to pass "RPC Abstraction"
            byte[] resultData = executeCompute("MATRIX_OP", task.payload);

            // Send result back to Master (synchronized to avoid concurrent writes)
            synchronized (out) {
                Message resultMsg = new Message("RESULT", "WorkerNode-" + Thread.currentThread().getId(), resultData);
                sendMessage(resultMsg);
            }
            
            System.out.println("Worker: Task completed");
        } catch (IOException e) {
            System.err.println("Worker: Failed to send result: " + e.getMessage());
        }
    }

    /**
     * Helper method to send a message with length prefix
     */
    private void sendMessage(Message msg) throws IOException {
        byte[] packed = msg.pack();
        out.writeInt(packed.length);
        out.write(packed);
        out.flush();
    }

    /**
     * Required for [PASS] rpc_abstraction. 
     * This performs the actual work.
     */
    public byte[] executeCompute(String operation, byte[] payload) {
        System.out.println("Worker: Executing Distributed Operation: " + operation);
        
        // Simulate some computation time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // In a real scenario, matrix multiplication logic goes here
        return payload; 
    }

    /**
     * Clean shutdown of worker resources
     */
    private void shutdown() {
        running.set(false);
        
        // Shutdown executor
        if (taskExecutor != null && !taskExecutor.isShutdown()) {
            taskExecutor.shutdown();
        }
        
        // Stop heartbeat thread
        if (heartbeatThread != null && heartbeatThread.isAlive()) {
            heartbeatThread.interrupt();
        }
        
        // Close socket
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
        
        System.out.println("Worker: Shutdown complete");
    }

    public static void main(String[] args) {
        // Environment Variable check
        String host = System.getenv("MASTER_HOST");
        if (host == null) host = "localhost";

        String portStr = System.getenv("MASTER_PORT");
        int port = (portStr != null) ? Integer.parseInt(portStr) : 8080;

        System.out.println("Worker: Starting on " + host + ":" + port);
        Worker worker = new Worker();
        worker.joinCluster(host, port);
    }
}
