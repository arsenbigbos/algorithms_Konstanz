package main;

import java.util.Random;
import java.util.concurrent.ExecutionException;

public class Tester1 {
    private static final int n = 100;
    private static final int maxP = 6;
    private static final int avgExp = 6;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        parallelMergeSort();
        parallelMergeSortMoreP();
        parallelQuickSort();
        parallelQuickSortMoreP();
    }

    public static void parallelMergeSort() throws InterruptedException {
        long start, end;
        int sum;

        System.out.println("Parallel MergeSort: Average running time");
        for (int p = 1; p <= maxP; p++) {
            sum = 0;
            for (int i = 0; i < n; i++) {
                int[] a = getDistinctRandomArray((int) Math.pow(10, avgExp));

                start = System.currentTimeMillis();
                a = Sorter.parallelMergeSort(a, p);
                end = System.currentTimeMillis();
                sum += end - start;

                assert (isSorted(a));
            }
            System.out.println(p + ": " + ((double) sum / n));
        }
        System.out.println();
    }

    public static void parallelMergeSortMoreP() throws InterruptedException {
        long start, end;
        int sum;

        System.out.println("Parallel MergeSort (more P): Average running time");
        for (int p = 1; p <= maxP; p++) {
            sum = 0;
            for (int i = 0; i < n; i++) {
                int[] a = getDistinctRandomArray((int) Math.pow(10, avgExp));

                start = System.currentTimeMillis();
                a = Sorter.parallelMergeSort(a, p, true);
                end = System.currentTimeMillis();
                sum += end - start;

                assert (isSorted(a));
            }
            System.out.println(p + ": " + ((double) sum / n));
        }
        System.out.println();
    }

    public static void parallelQuickSort() throws InterruptedException, ExecutionException {
        long start, end;
        int sum;

        System.out.println("Parallel QuickSort: Average running time");
        for (int p = 1; p <= maxP; p++) {
            sum = 0;
            for (int i = 0; i < n; i++) {
                int[] a = getDistinctRandomArray((int) Math.pow(10, avgExp));

                start = System.currentTimeMillis();
                a = Sorter.parallelQuickSort(a, p);
                end = System.currentTimeMillis();
                sum += end - start;

                assert (isSorted(a));
            }
            System.out.println(p + ": " + ((double) sum / n));
        }
        System.out.println();
    }

    public static void parallelQuickSortMoreP() throws InterruptedException, ExecutionException {
        long start, end;
        int sum;

        System.out.println("Parallel QuickSort (more P): Average running time");
        for (int p = 1; p <= maxP; p++) {
            sum = 0;
            for (int i = 0; i < n; i++) {
                int[] a = getDistinctRandomArray((int) Math.pow(10, avgExp));

                start = System.currentTimeMillis();
                a = Sorter.parallelQuickSort(a, p, true);
                end = System.currentTimeMillis();
                sum += end - start;

                assert (isSorted(a));
            }
            System.out.println(p + ": " + ((double) sum / n));
        }
        System.out.println();
    }

    public static int[] getDistinctRandomArray(int n) {
        Random rand = new Random();
        int[] a = new int[n];
        for (int i = 0; i < n; i++) {
            a[i] = i;
        }
        int j = 0;
        for (int i = 0; i < n; i++) {
            j = rand.nextInt(0, n);
            Sorter.swap(a, i, j);
        }

        return a;
    }

    public static boolean isSorted(int[] a) {
        if (a == null) {
            return false;
        }
        if (a.length <= 1) {
            return true;
        }

        int last = a[0];
        for (int i = 1; i < a.length; i++) {
            if (last > a[i]) {
                return false;
            }
            last = a[i];
        }
        return true;
    }
}

