package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.*;

public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final CopyOnWriteArrayList<WorkerConnection> activeWorkers = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, WorkerConnection> workerMap = new ConcurrentHashMap<>();
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<Integer, Task> pendingTasks = new ConcurrentHashMap<>();
    private ServerSocket serverSocket;
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger taskIdGenerator = new AtomicInteger(0);
    private Thread heartbeatMonitor;
    private volatile boolean running = true;

    /**
     * WorkerConnection class to track individual worker state
     */
    private class WorkerConnection {
        final Socket socket;
        final DataInputStream in;
        final DataOutputStream out;
        final String workerId;
        final AtomicLong lastHeartbeat;
        final Set<Integer> assignedTasks;
        volatile boolean active;

        WorkerConnection(Socket socket, String workerId) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new DataOutputStream(socket.getOutputStream());
            this.workerId = workerId;
            this.lastHeartbeat = new AtomicLong(System.currentTimeMillis());
            this.assignedTasks = ConcurrentHashMap.newKeySet();
            this.active = true;
        }

        void updateHeartbeat() {
            lastHeartbeat.set(System.currentTimeMillis());
        }

        long getTimeSinceLastHeartbeat() {
            return System.currentTimeMillis() - lastHeartbeat.get();
        }

        synchronized void sendMessage(Message msg) throws IOException {
            byte[] packed = msg.pack();
            out.writeInt(packed.length);
            out.write(packed);
            out.flush();
        }

        void close() {
            active = false;
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    /**
     * Task representation for tracking and recovery
     */
    private static class Task {
        final int taskId;
        final byte[] data;
        int attempts;

        Task(int taskId, byte[] data) {
            this.taskId = taskId;
            this.data = data;
            this.attempts = 0;
        }
    }

    /**
     * Entry point for a distributed computation.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        completedTasks.incrementAndGet();
        System.out.println("Master: Coordinating " + operation + " (Task #" + completedTasks.get() + ")");
        
        // Create tasks and distribute to workers
        for (int i = 0; i < workerCount; i++) {
            int taskId = taskIdGenerator.incrementAndGet();
            byte[] taskData = ("TASK_" + taskId).getBytes();
            Task task = new Task(taskId, taskData);
            taskQueue.offer(task);
        }
        
        return null; 
    }

    /**
     * Start the communication listener.
     */
    public void listen(int port) throws IOException {
        this.serverSocket = new ServerSocket(port);
        System.out.println("Master: Server started on port " + port);

        // Start heartbeat monitor thread
        startHeartbeatMonitor();

        // Start task dispatcher thread
        startTaskDispatcher();

        // Accept worker connections
        systemThreads.submit(() -> {
            try {
                while (running && serverSocket != null && !serverSocket.isClosed()) {
                    Socket workerSocket = serverSocket.accept();
                    System.out.println("Master: New worker connection established");
                    handleWorker(workerSocket);
                }
            } catch (IOException e) {
                if (running && serverSocket != null && !serverSocket.isClosed()) {
                    System.err.println("Master Listener Error: " + e.getMessage());
                }
            }
        });
    }

    /**
     * CRITICAL: Heartbeat monitoring thread for failure detection
     */
    private void startHeartbeatMonitor() {
        heartbeatMonitor = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(5000); // Check every 5 seconds
                    
                    List<WorkerConnection> failedWorkers = new ArrayList<>();
                    
                    // Check each worker's heartbeat
                    for (WorkerConnection worker : activeWorkers) {
                        long timeSinceHeartbeat = worker.getTimeSinceLastHeartbeat();
                        
                        // 10-second timeout threshold
                        if (timeSinceHeartbeat > 10000) {
                            System.err.println("Master: Worker " + worker.workerId + " timeout detected (no heartbeat for " + timeSinceHeartbeat + "ms)");
                            failedWorkers.add(worker);
                        }
                    }
                    
                    // Handle failed workers
                    for (WorkerConnection worker : failedWorkers) {
                        handleWorkerFailure(worker);
                    }
                    
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatMonitor.setDaemon(true);
        heartbeatMonitor.start();
    }

    /**
     * CRITICAL: Task dispatcher for distributing work
     */
    private void startTaskDispatcher() {
        systemThreads.submit(() -> {
            while (running) {
                try {
                    Task task = taskQueue.poll(1, TimeUnit.SECONDS);
                    if (task == null) continue;
                    
                    // Find available worker
                    WorkerConnection worker = findAvailableWorker();
                    if (worker != null) {
                        assignTaskToWorker(worker, task);
                    } else {
                        // No workers available, put back in queue
                        taskQueue.offer(task);
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
    }

    private WorkerConnection findAvailableWorker() {
        for (WorkerConnection worker : activeWorkers) {
            if (worker.active && worker.assignedTasks.size() < 3) {
                return worker;
            }
        }
        return null;
    }

    private void assignTaskToWorker(WorkerConnection worker, Task task) {
        try {
            task.attempts++;
            worker.assignedTasks.add(task.taskId);
            pendingTasks.put(task.taskId, task);
            
            Message taskMsg = new Message("TASK", "MASTER", task.data);
            worker.sendMessage(taskMsg);
            
            System.out.println("Master: Assigned task " + task.taskId + " to worker " + worker.workerId);
        } catch (IOException e) {
            System.err.println("Master: Failed to assign task to worker: " + e.getMessage());
            handleWorkerFailure(worker);
        }
    }

    /**
     * CRITICAL: Worker failure recovery mechanism
     */
    private void handleWorkerFailure(WorkerConnection worker) {
        System.out.println("Master: Handling failure of worker " + worker.workerId);
        
        // Remove from active workers
        activeWorkers.remove(worker);
        workerMap.remove(worker.workerId);
        
        // Recover tasks assigned to this worker
        Set<Integer> lostTasks = new HashSet<>(worker.assignedTasks);
        for (Integer taskId : lostTasks) {
            Task task = pendingTasks.get(taskId);
            if (task != null) {
                System.out.println("Master: Recovering task " + taskId + " from failed worker");
                pendingTasks.remove(taskId);
                
                // Re-queue the task if not attempted too many times
                if (task.attempts < 3) {
                    taskQueue.offer(task);
                } else {
                    System.err.println("Master: Task " + taskId + " exceeded max retry attempts");
                }
            }
        }
        
        // Close worker connection
        worker.close();
        
        // Reconcile state
        reconcileState();
    }

    private void handleWorker(Socket socket) {
        systemThreads.submit(() -> {
            WorkerConnection worker = null;
            try {
                // Set socket timeout for read operations
                socket.setSoTimeout(3000);
                
                // Wait for initial JOIN message
                DataInputStream tempIn = new DataInputStream(socket.getInputStream());
                int length = tempIn.readInt();
                byte[] buffer = new byte[length];
                tempIn.readFully(buffer);
                Message joinMsg = Message.unpack(buffer);
                
                if (joinMsg == null || !"JOIN".equals(joinMsg.messageType)) {
                    socket.close();
                    return;
                }
                
                // Create worker connection
                String workerId = joinMsg.sender;
                worker = new WorkerConnection(socket, workerId);
                activeWorkers.add(worker);
                workerMap.put(workerId, worker);
                
                System.out.println("Master: Worker " + workerId + " joined (total workers: " + activeWorkers.size() + ")");
                
                // Send ACK
                Message ack = new Message("ACK", "MASTER", "OK".getBytes());
                worker.sendMessage(ack);
                
                // Listen for messages from this worker
                while (worker.active && !socket.isClosed()) {
                    try {
                        length = worker.in.readInt();
                        buffer = new byte[length];
                        worker.in.readFully(buffer);
                        Message msg = Message.unpack(buffer);
                        
                        if (msg == null) continue;
                        
                        // Handle different message types
                        if ("HEARTBEAT".equals(msg.messageType)) {
                            worker.updateHeartbeat();
                            System.out.println("Master: Heartbeat received from " + workerId);
                        } else if ("RESULT".equals(msg.messageType)) {
                            handleTaskResult(worker, msg);
                        } else if ("PONG".equals(msg.messageType)) {
                            worker.updateHeartbeat();
                        }
                        
                    } catch (SocketTimeoutException e) {
                        // Check if worker is still alive with ping
                        try {
                            Message ping = new Message("PING", "MASTER", new byte[0]);
                            worker.sendMessage(ping);
                        } catch (IOException ioE) {
                            // Worker is dead
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Master: Worker connection error: " + e.getMessage());
            } finally {
                if (worker != null) {
                    handleWorkerFailure(worker);
                }
            }
        });
    }

    private void handleTaskResult(WorkerConnection worker, Message msg) {
        // Extract task ID from payload (simplified)
        System.out.println("Master: Received result from worker " + worker.workerId);
        
        // Remove from worker's assigned tasks
        // In real implementation, you'd parse task ID from message
        if (!worker.assignedTasks.isEmpty()) {
            Integer taskId = worker.assignedTasks.iterator().next();
            worker.assignedTasks.remove(taskId);
            pendingTasks.remove(taskId);
            completedTasks.incrementAndGet();
        }
    }

    public void reconcileState() {
        // Clean up logic
        activeWorkers.removeIf(w -> !w.active || w.socket.isClosed());
        System.out.println("Master: Cluster state reconciled. Active workers: " + activeWorkers.size());
    }

    public void shutdown() {
        running = false;
        
        // Send shutdown to all workers
        for (WorkerConnection worker : activeWorkers) {
            try {
                Message shutdown = new Message("SHUTDOWN", "MASTER", new byte[0]);
                worker.sendMessage(shutdown);
            } catch (IOException e) {
                // Ignore
            }
            worker.close();
        }
        
        systemThreads.shutdown();
        
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
    }

    public static void main(String[] args) {
        // Environment variable check
        String envPort = System.getenv("MASTER_PORT");
        int port = (envPort != null) ? Integer.parseInt(envPort) : 8080;

        Master master = new Master();
        try {
            master.listen(port);
            
            // Keep running
            Thread.currentThread().join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
