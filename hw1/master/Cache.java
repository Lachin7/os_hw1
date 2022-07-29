package os.hw1.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;


public class Cache {

    private int port;
    private boolean isRunning = true;
    private Socket socket;
    private Scanner in;
    private PrintStream out;
    private HashMap<String, Integer> queue;

    public Cache(int port) {
        this.port = port;
        queue = new HashMap<>();
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        new Cache(port).start();
    }

    private void start() {
        isRunning = true;

        try {
            socket = new Socket(InetAddress.getLocalHost(), port);
            in = new Scanner(socket.getInputStream());
            out = new PrintStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        new Thread(() -> {
            try {
                listenForConnections();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

    }

    private void listenForConnections() throws IOException {
        while (isRunning) {
            String msg = in.nextLine();
            if(msg.contains("check for")){
                String[] res = msg.split("#");
                int i = Integer.parseInt(res[1]);
                int x = Integer.parseInt(res[2]);
                if(queue.get(i+"#"+x) != null){
                    out.println("output is #"+i+"#"+x+"#"+queue.get(i+"#"+x));
                } else {
                    out.println("not in cache #"+i+"#"+x);
                }
            } else if (msg.contains("new query")){
                String[] res = msg.split("#");
                int i = Integer.parseInt(res[1]);
                int x = Integer.parseInt(res[2]);
                int y = Integer.parseInt(res[3]);
                queue.put(i+"#"+x, y);
            }
        }
    }


}
