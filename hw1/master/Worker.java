package os.hw1.master;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Worker {

    private int port;
    private boolean isRunning;
    private ServerSocket socket;
    private List<String> commonArgs;

    public Worker(int port, List<String> commonArgs) {
        this.port = port;
        this.commonArgs = commonArgs;
    }


    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int argsNum = Integer.parseInt(args[1]);
        List<String> command = new LinkedList<>(Arrays.asList(args).subList(2, argsNum + 2));
        new Worker(port,command).start();
    }

    private void start() {
        isRunning = true;

        try {
            socket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (isRunning){
            try {
                Socket s = socket.accept();
                listenForConnections(s);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void listenForConnections(Socket s) throws IOException {
        new Thread(() -> {
            try {
                PrintStream out = new PrintStream(s.getOutputStream());
                Scanner in = new Scanner(s.getInputStream());
                String programAddress = in.next();
                int input = in.nextInt();
                initializeProgram(programAddress, input, s, out);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private void initializeProgram(String programAddress, int input, Socket s, PrintStream out){
        List<String> args = new LinkedList<>(commonArgs);
        args.add(programAddress);
        ProcessBuilder processBuilder = new ProcessBuilder(args);
        try {
            Process process = processBuilder.start();
            PrintStream printStream = new PrintStream(process.getOutputStream());
            Scanner scanner = new Scanner(process.getInputStream());
            printStream.println(input);
            printStream.flush();
            int res = scanner.nextInt();
            out.println(res);
            s.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
