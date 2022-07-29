package os.hw1.master;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.util.LinkedList;

public class ExecutionChain {
    private String chain;
//    private PrintStream out;
    private long id, time;

    private int input;
    private Socket client;

    private LinkedList<Integer> programs;

    public ExecutionChain(String chain, Socket client) {
        this.chain = chain;
//        this.out = out;
        this.id = System.currentTimeMillis();
        this.time = System.nanoTime();
        this.client = client;
        programs = new LinkedList<>();
        String[] arr = chain.split("\\|");
        for (int i = 0; i < arr.length; i++) {
            if (i == arr.length - 1) {
                programs.add(Integer.parseInt(arr[i].split(" ")[0]));
                input = Integer.parseInt(arr[i].split(" ")[1]);
            } else programs.add(Integer.valueOf(arr[i]));
        }
    }

    public int getSize() {
        return programs.size();
    }

    public int getLastProgram() {
        return programs.getLast();
    }

    public int getInput() {
        return input;
    }

    public void applyResult(int y) {
        programs.pop();
        this.input = y;
    }

    public PrintStream getOut() {
        try {
            return new PrintStream(client.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getTime() {
        return this.time;
    }

    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "ExecutionChain{" +
                "programs='" + programs + '\'' +
                "input=' " + input + '\'' +
                ", id=" + id +
                '}';
    }

    public Socket getClient() {
        return client;
    }

    public void setInput(int input) {
        this.input = input;
    }
}
