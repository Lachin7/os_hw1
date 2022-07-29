package os.hw1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class Tester {
    public static final int WAIT_P1 = 1000;
    public static final int WAIT_P2 = 2000;
    static final long SAFA_MARGIN = 300;
    static final int port = 13280;
    static final int workerCount = 2;
    static final int w = 5;
    static final String[] commonArgs = {
            "/Users/backup/Library/Java/JavaVirtualMachines/corretto-11.0.16/Contents/Home/bin/java",
            // "/home/sam/.jdks/corretto-1.8.0_322/bin/java", // replace with your java path with version 1.8
            "-classpath",
            "out/production/os/"  // replace with your classpath
    };
    static final String[] programs = {
            "os.hw1.programs.Program1 2",
            "os.hw1.programs.Program2 3",
    };

    static final ExecutorService executorService = Executors.newFixedThreadPool(50);

    static Process runProcess() throws Exception {
        Process process = new ProcessBuilder(
                commonArgs[0], commonArgs[1], commonArgs[2], "os.hw1.master.MasterMain"
        ).start();
        PrintStream printStream = new PrintStream(process.getOutputStream());
        printStream.println(port);
        printStream.println(workerCount);
        printStream.println(w);
        printStream.println(commonArgs.length);
        Arrays.stream(commonArgs).forEach(printStream::println);
        printStream.println(programs.length);
        Arrays.stream(programs).forEach(printStream::println);
        printStream.flush();
        Scanner scanner = new Scanner(process.getInputStream());
        scanner.nextLine(); // wait for first line of output to ensure that master initialization is completed

        new Thread(() -> transfer(System.err, process.getErrorStream())).start();
        return process;
    }

    static void transfer(OutputStream outputStream, InputStream inputStream) {
        Scanner scanner = new Scanner(inputStream);
        PrintStream printStream = new PrintStream(outputStream);
        while (scanner.hasNextLine()) {
            printStream.println(scanner.nextLine());
            printStream.flush();
        }
    }

    static class Response {
        private final long time;
        private final int output;

        Response(long time, int output) {
            this.time = time;
            this.output = output;
        }
    }

    private static Response sendRequest(int input, int... programs) {
        String request = Arrays.stream(programs).mapToObj(String::valueOf).collect(Collectors.joining("|")) + " " + input;
        try {
            long start = System.currentTimeMillis();
            Socket socket = new Socket(InetAddress.getLocalHost(), port);
            Scanner scanner = new Scanner(socket.getInputStream());
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(request);
            int response = scanner.nextInt();
            socket.close();
            return new Response(System.currentTimeMillis() - start, response);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    static void assertTime(long actual, long expected) throws Exception {
        System.out.println(actual - expected);
        if (expected - SAFA_MARGIN > actual || actual > expected + SAFA_MARGIN) {
            throw new Exception("boom");
        }
    }

    static void assertInt(int actual, int expected) throws Exception {
        if (actual != expected) {
            throw new Exception("boom");
        }
    }

    static void assertString(String actual, String expected) throws Exception {
        if (!Objects.equals(actual, expected)) {
            throw new Exception("boom");
        }
    }


    public static void main(String[] args) throws Exception {
        Process process = runProcess();
        int a = 5;
        int[] programs = new int[a];
        Arrays.fill(programs, 1);
        Future<Response> r1 = executorService.submit(() -> sendRequest(50, programs));
        Thread.sleep(100);
        Future<Response> r2 = executorService.submit(() -> sendRequest(50, programs));
        Thread.sleep(100);
        Future<Response> r3 = executorService.submit(() -> sendRequest(50, programs));
        Response result1 = r1.get();
        assertTime(result1.time, a * WAIT_P1);
        assertInt(result1.output, 50 - a);
        Response result2 = r2.get();
        assertTime(result2.time, a * WAIT_P1 - 100);
        assertInt(result2.output, 50 - a);
        Response result3 = r3.get();
        assertTime(result3.time, a * WAIT_P1 - 200);
        assertInt(result3.output, 50 - a);
        System.out.println("pass phase 1");

        Future<Response> ra1 = executorService.submit(() -> sendRequest(10, 2));
        Thread.sleep(50);
        Future<Response> ra2 = executorService.submit(() -> sendRequest(13, 2));
        Thread.sleep(50);
        Future<Response> ra3 = executorService.submit(() -> sendRequest(17, 2));
        Response resulta1 = ra1.get();
        assertTime(resulta1.time, WAIT_P2);
        assertInt(resulta1.output, 0);
        Response resulta2 = ra2.get();
        assertTime(resulta2.time, WAIT_P2);
        assertInt(resulta2.output, 1);
        Response resulta3 = ra3.get();
        assertTime(resulta3.time, 2 * WAIT_P2);
        assertInt(resulta3.output, 3);
        System.out.println("phase 2 done");
        process.destroy();
        process.waitFor();
        executorService.shutdown();
    }

}


