package main;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

public class Tester {
    public static void main(String[] args) throws IOException {
        String origFile = "data/input.bin";
        String tmpFile = "data/tmp.bin";
        String outFile = "data/output.bin";
        String compFile = "data/comp.bin";

        // these values are small, so we can compare the file to one which is sorted with a standard sorting
        // algorithm while also having the EM-MergeSort perform multiple rounds
        int blockSizeMiB = 32;
        int ramSizeMiB = 4 * blockSizeMiB;
        int fileSize = ramSizeMiB * 10;

        Sorter.createRandomFile(origFile, fileSize, ramSizeMiB);
        Files.copy(Paths.get(origFile), Paths.get(tmpFile), StandardCopyOption.REPLACE_EXISTING);

        Sorter.sortFileComp(origFile, compFile);
        testEM(tmpFile, outFile, blockSizeMiB, ramSizeMiB);


        System.out.println("Is sorted: " + isSorted(outFile, ramSizeMiB));
        System.out.println("Files are the same: " + compareFiles(new File(compFile), new File(outFile), 512));
    }

    public static boolean compareFiles(File file1, File file2, int ramSizeMiB) throws IOException {
        DataInputStream in1 = new DataInputStream(new BufferedInputStream(new FileInputStream(file1)));
        DataInputStream in2 = new DataInputStream(new BufferedInputStream(new FileInputStream(file2)));

        if (file1.length() != file2.length()) {
            return false;
        }

        byte[] arr1;
        byte[] arr2;
        int M = ramSizeMiB / 2 * Sorter.MIB_TO_B;

        while ((arr1 = in1.readNBytes(M)).length > 0 && (arr2 = in2.readNBytes(M)).length > 0) {
            if (!Arrays.equals(arr1, arr2)) {
                return false;
            }
        }

        in1.close();
        in2.close();

        return true;
    }

    public static long testEM(String inFileName, String outFileName, int blockSizeMiB, int ramSizeMiB)
            throws IOException {
        long startTime = System.currentTimeMillis();

        Sorter sorter = new Sorter(ramSizeMiB);
        int rounds = sorter.emMergeSort(inFileName, outFileName, blockSizeMiB);

        long endTime = System.currentTimeMillis();
        long diff = endTime - startTime;
        int millis = (int) (diff % 1000);
        int seconds = (int) ((diff / 1000) % 60);
        int minutes = (int) (diff / 60000);
        System.out.printf("EM-MergeSort: %d'%03d.%03d\n", minutes, seconds, millis);
        System.out.println("rounds: " + rounds);

        return diff;
    }

    public static long testClassical(String inFileName, String outFileName) throws IOException {
        long startTime = System.currentTimeMillis();

        Sorter.mergeSortOnFile(inFileName, outFileName);

        long endTime = System.currentTimeMillis();
        long diff = endTime - startTime;
        int millis = (int) (diff % 1000);
        int seconds = (int) ((diff / 1000) % 60);
        int minutes = (int) (diff / 60000);
        System.out.printf("Classical MergeSort: %d'%03d.%03d\n", minutes, seconds, millis);

        return diff;
    }



    public static boolean isSorted(String fileName, int ramSizeMiB) throws IOException {
        DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));

        int last = Integer.MIN_VALUE;
        int current;

        byte[] byteArray;
        ByteBuffer buffer;
        int M = ramSizeMiB * Sorter.MIB_TO_B;

        while ((byteArray = in.readNBytes(M)).length > 0) {
            buffer = ByteBuffer.wrap(byteArray);
            current = buffer.asIntBuffer().get();

            if (current < last) {
                in.close();
                return false;
            }

            last = current;
        }

        in.close();
        return true;
    }
}
