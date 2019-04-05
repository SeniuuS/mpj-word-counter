import mpi.*;

import java.io.*;

public class CountWordMpj {

    public static int actualCore = 0;
    public static String fileName;
    public static String word;
    public static Thread receiveThread;
    public static int numberOfWord = 0;

    public static void main(String args[]) throws IOException {
        String[] appArgs = MPI.Init(args);
        fileName = appArgs[0];
        word = appArgs[1];
        int me = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        if(me == 0) {
            launchReceiveCountThread();
            readFile(fileName);
            System.out.println("Number of word " + numberOfWord);
            receiveThread.interrupt();
        }else{
            receive();
        }
        MPI.Finalize();
    }

    public static void receive(){
        while(true) {
            int count[] = new int[1];
            MPI.COMM_WORLD.Recv(count, 0, 1, MPI.INT, 0, 98);

            if (count[0] == 0) {
                return;
            }

            char[] line = new char[count[0]];
            MPI.COMM_WORLD.Recv(line, 0, count[0], MPI.CHAR, 0, 99);

            int nb = count(word, new String(line));

            int nbToSend[] = new int[1];
            count[0] = nb;
            MPI.COMM_WORLD.Send(count, 0, 1, MPI.INT, 0, 97);
        }
    }

    public static int count(String word, String line){
        String a[] = line.split(" ");

        int count = 0;
        for (int i = 0; i < a.length; i++)
        {
            if (a[i].toLowerCase().contains(word.toLowerCase()))
                count++;
        }

        return count;
    }

    public static void readFile(String fileName) throws IOException {
        FileInputStream fstream = new FileInputStream(fileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String strLine;

        while ((strLine = br.readLine()) != null)   {
            send(strLine);
        }

        for(int i = 1; i < MPI.COMM_WORLD.Size(); i++){
            int count[] = new int[1];
            count[0] = 0;
            MPI.COMM_WORLD.Send(count, 0, 1, MPI.INT, i, 98);
        }

        fstream.close();
    }

    public static void send(String line){
        int coreToSend = actualCore+1;
        if(coreToSend >= MPI.COMM_WORLD.Size()){
            coreToSend = 1;
        }

        int count[] = new int[1];
        count[0] = line.toCharArray().length;
        MPI.COMM_WORLD.Send(count, 0, 1, MPI.INT, coreToSend, 98);
        MPI.COMM_WORLD.Send(line.toCharArray(), 0, line.toCharArray().length, MPI.CHAR, coreToSend, 99);

        actualCore = coreToSend;
    }

    public static void launchReceiveCountThread(){
        receiveThread = new Thread(){
            @Override
            public void run(){
                try {
                    while (true) {
                        int count[] = new int[1];
                        MPI.COMM_WORLD.Recv(count, 0, 1, MPI.INT, MPI.ANY_SOURCE, 97);
                        numberOfWord += count[0];
                    }
                }catch(Exception e){

                }
            }
        };
        receiveThread.start();
    }

}
