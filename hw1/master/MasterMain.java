package os.hw1.master;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class MasterMain implements Runnable {

    private final int port, cacheSidePort, n, w, numberOfArgs;
    private ServerSocket clientSideSocket, workerSideSocket, cacheSideSocket;
    private Scanner workerIn, cacheIn;
    private PrintStream workerOut, cacheOut;
    private String[] programs;
    private List<String> commonArgs;
    private Integer[] programWeights;
    private LinkedList<Integer> workersWeights;
    private Socket cache, worker;
    private boolean isRunning;
    private HashMap<Integer, Process> workers;
    private volatile PriorityQueue<ExecutionChain> chains;
    private volatile LinkedList<ExecutionChain> pending;
    private List<String> waitingToBeCalculated;
    private HashMap<String, Integer> cacheQueue;
    private Thread calculationThread;
    private long masterPID;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int port = scanner.nextInt();
        // number of workers
        int n = scanner.nextInt();
        // max weight size on one server
        int w = scanner.nextInt();
        // common args length:
        int numArgs = scanner.nextInt();
        List<String> commonArgs = new LinkedList<>();
        for (int i = 0; i < numArgs; i++) {
            commonArgs.add(scanner.next());
        }
        // number of programs
        int k = scanner.nextInt();
        String[] programs = new String[k];
        scanner.nextLine();
        for (int i = 0; i < k; i++) {
            programs[i] = scanner.nextLine();
        }

        // initialize master
        long masterPID = ProcessHandle.current().pid();
        System.out.println("master "+"start"+" "+masterPID+" "+port);
        MasterMain master = new MasterMain(masterPID, port, n, w, numArgs, commonArgs, programs);
        master.start();
    }

    public MasterMain(long masterPID, int port, int n, int w, int numberOfArgs, List<String> commonArgs, String[] programs) {
        this.masterPID = masterPID;
        this.port = port;
        System.err.println("port number: "+port);
        cacheSidePort = port - 50;
//        workerSidePort = port + 50;
        this.n = n;
        this.w = w;
        this.numberOfArgs = numberOfArgs;
        this.commonArgs = commonArgs;
        this.programs = programs;
        workers = new HashMap<>();
//        workerHandlers = new ArrayList<>();
        waitingToBeCalculated = new ArrayList<>();
        cacheQueue = new HashMap<>();
        chains = new PriorityQueue<>(Comparator.comparing(ExecutionChain::getTime));
        pending = new LinkedList<>();
        initializeCache();
        // todo the place for initializing workers
        initializeWeights();
        initializeWorkers();
        System.err.println("number of workers: "+n);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for(Process p: workers.values()) p.destroy();
            try {
                clientSideSocket.close();
                cacheSideSocket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void start() {
        initSockets();
        isRunning = true;
        new Thread(this).start();
    }


    private void initializeWorkers() {
        for (int i = 0; i < n; i++) {
            List<String> args = new LinkedList<>(commonArgs);
            args.add(Worker.class.getName());

            int port_i = i + 1000 + port;
            args.add(String.valueOf(port_i));
            args.add(String.valueOf(numberOfArgs));
            for (int j = 0; j < numberOfArgs; j++) {
                args.add(commonArgs.get(j));
            }

            ProcessBuilder processBuilder = new ProcessBuilder(args);
            try {
                Process process = processBuilder.start();
                long pid = process.pid();
                System.out.println("worker " + i + " start " + pid + " " + port_i);
                workers.put(port_i, process);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.err.println("workers are initialized");
    }

    private void initializeCache() {
        List<String> args = new LinkedList<>(commonArgs);
        args.add(Cache.class.getName());
        args.add(String.valueOf(cacheSidePort));

        ProcessBuilder processBuilder = new ProcessBuilder(args);
        try {
            Process process = processBuilder.start();
            long pid = process.pid();
            System.out.println("cache start " + pid + " " + cacheSidePort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initializeWeights() {
        programWeights = new Integer[programs.length];
        for (int i = 0; i < programs.length; i++) {
            programWeights[i] = Integer.valueOf(programs[i].split(" ")[1]);
            programs[i] = programs[i].split(" ")[0];
        }
        workersWeights = new LinkedList<>();
        for (int i = 0; i < n; i++) {
            workersWeights.add(0);
        }
        System.err.println("weights are initialized");
    }

    @Override
    public void run() {
//        initCacheSide();
        initClientSide();
//        applyCacheOnChains();
        calculate();
//        calculationThread = new Thread(this::calculate);
//        calculationThread.start();
    }

    private void applyCacheOnChains() {
        new Thread(() -> {
            while (isRunning) {
                for (ExecutionChain chain : chains) {
                    try {
                        int i = chain.getLastProgram();
                        int x = chain.getLastProgram();

                        if (cacheQueue.get(i + "#" + x) != null && cacheQueue.get(i + "#" + x) != Integer.MAX_VALUE) {
                            chain.applyResult(cacheQueue.get(i + "#" + x));
                        } else if (cacheQueue.get(i + "#" + x) == null && cacheOut != null) {
                            askExistenceInCache(i, x);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    private void calculate() {
        while (isRunning) {
            synchronized (chains) {
                if (chains.size() != 0) {
                    ExecutionChain chain = chains.element();
//                    System.err.println(chain);
                    int p = chain.getLastProgram();
                    int r = programWeights[p - 1];
                    // find the worker with the1658849323191 smallest weight:
                    int worker = workersWeights.indexOf(Collections.min(workersWeights));
                    if(worker!=-1 && workersWeights.get(worker) + r <= w) {
                        workersWeights.set(worker, workersWeights.get(worker) + programWeights[p-1]);
                        chains.poll();
//                        System.err.println("worker with smallest weight "+worker+"  "+workersWeights[worker]);

                        passToWorker(chain, worker);
                    }
                }
                try {
                    chains.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


//                        System.err.println(cacheQueue.get(i + "#" + x) != null);
//                    System.err.println(cacheQueue);

    //                    if (workerHandler != null && r + workerHandler.getWeights() <= this.w && cacheQueue.get(i + "#" + x) != null && cacheQueue.get(i + "#" + x) != Integer.MAX_VALUE) {
//                        pending.add(chain);
//                        chains.remove(chain);
//                        workerHandler.calculate(r, i, x, chain);
//                    } else {
//                        waitingWeight = r;
//                        try {
//                            l2.wait();
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
    private void passToWorker(ExecutionChain chain, int worker) {
        new Thread(() -> {
            try {
//                System.err.println("chain "+ chain.getId()+" passed to  worker, let's connect to it"+" worker: "+worker);
                Socket s = new Socket(InetAddress.getLocalHost(),worker+port+1000);
                int p = chain.getLastProgram();
//                System.err.println("chain last program: "+p+" worker: "+worker);
                int i = chain.getInput();
//                System.err.println("chain last input "+i+" worker: "+worker);
                PrintStream workerOut = new PrintStream(s.getOutputStream());
                Scanner workerIn = new Scanner(s.getInputStream());
                workerOut.println(programs[p-1]);
//                System.err.println("address: "+programs[p-1]);
                workerOut.println(i);
//                System.err.println(i);
                workerOut.flush();

                int res = workerIn.nextInt();
//                System.err.println("result calculated: "+res+" worker: "+worker+" programs size: "+chain.getSize());
                if (chain.getSize() == 1){
//                    System.err.println("programs should be size zero here");
//                    System.err.println("chain "+chain.getId()+" is done");
//                    System.err.println("result: "+res);
                    chain.getOut().println(res);
                    workerOut.flush();
                    chain.getClient().close();
                    synchronized (chains){
                        chains.notifyAll();
                    }
                } else {
                    synchronized (chains) {
                        chain.applyResult(res);
//                        System.err.println("chain after applying result "+chain);
                        chains.add(chain);
                        chains.notifyAll();
                    }
                }
                workersWeights.set(worker, workersWeights.get(worker) - programWeights[p-1]);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private int indexOfSmallest(Integer[] array) {
        if (array.length == 0)
            return -1;
        int index = 0;
        int min = array[index];
        for (int i = 1; i < array.length; i++){
            if (array[i] <= min){
                min = array[i];
                index = i;
            }
        }
        return index;
    }

//    private void checkOnWaitingWeight() {
//        int min = Integer.MAX_VALUE;
//        for (WorkerHandler handler : workerHandlers) {
//            if (handler.getWeights() < min) {
//                min = handler.getWeights();
//            }
//        }
//    }

//    private void checkWorkersAliveness() {
//        for (WorkerHandler workerHandler : workerHandlers) {
//            if (!workerHandler.checkIfIsAlive()) {
//                int id = workerHandler.getWorkerId();
//                long pid = workers.get(id).pid();
//                System.out.println("worker " + id + " stop " + pid + " " + (10000 + id));
//                ;
//            }
//        }
//    }

    private void initCacheSide() {
        new Thread(() -> {
            try {
                cache = this.cacheSideSocket.accept();
                cacheIn = new Scanner(cache.getInputStream());
                cacheOut = new PrintStream(cache.getOutputStream());
                new Thread(this::listenOnCache).start();
                System.err.println("cache socket getting created");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void askExistenceInCache(int i, int x) throws IOException {
        cacheOut.println("check for #" + i + "#" + x);
    }


    private void insertInCache(int i, int x, int y) throws IOException {
        cacheOut.println("new query #" + i + "#" + x + "#" + y);
    }


    private void listenOnCache() {
        while (isRunning && cacheIn.hasNextLine()) {
            String msg = cacheIn.nextLine();
            if (msg.contains("output is")) {
                String[] res = msg.split("#");
                int i = Integer.parseInt(res[1]);
                int x = Integer.parseInt(res[2]);
                int y = Integer.parseInt(res[3]);
                cacheQueue.put(i + "#" + x, y);
            } else if (msg.contains("not in cache")) {
                String[] res = msg.split("#");
                int i = Integer.parseInt(res[1]);
                int x = Integer.parseInt(res[2]);
                cacheQueue.put(i + "#" + x, Integer.MAX_VALUE);
            } else if (msg.contains("log")) {
                System.err.println(msg);
            }
        }
    }


    private void initWorkerSide() {
        new Thread(() -> {
            while (isRunning) {
                try {
                    worker = workerSideSocket.accept();

                    workerIn = new Scanner(worker.getInputStream());
                    workerOut = new PrintStream(worker.getOutputStream());

//                    workerHandler = new WorkerHandler(this, worker, workerIn, workerOut);
//                    workerHandlers.add(workerHandler);
//                    //todo:
//                    checkOnWaitingWeight();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void initClientSide() {
        new Thread(() -> {
            while (isRunning) {
                try {
                    Socket client = clientSideSocket.accept();
                    processClientSide(client);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start();
    }

    private void processClientSide(Socket client) {
//        new Thread(() -> {
            try {
                Scanner clientIn = new Scanner(client.getInputStream());
                String chain = clientIn.nextLine();
//                PrintStream clientOut = new PrintStream(client.getOutputStream());
                synchronized (chains) {
                    ExecutionChain executionChain = new ExecutionChain(chain, client);
                    chains.add(executionChain);
//                    System.err.println(executionChain);
                    chains.notifyAll();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
//        }).start();
    }


    private void initSockets() {
        try {
            clientSideSocket = new ServerSocket(port);
            cacheSideSocket = new ServerSocket(cacheSidePort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        System.out.println("master " + "stop" + " " + masterPID + " " + port);
        try {
            clientSideSocket.close();
            workerSideSocket.close();
            cacheSideSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public synchronized void notifyRes(int i, int x, int y, long id) {
        System.err.println("notify res " + "i :" + i + " x:" + x + " y:" + y + " id" + id);
        for (ExecutionChain chain : pending) {
            if (chain.getId() == id) {
                chain.applyResult(y);
                try {
                    insertInCache(i, x, y);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                pending.remove(chain);
                if (chain.getSize() > 1) chains.add(chain);
                break;
            }
        }
    }
}
