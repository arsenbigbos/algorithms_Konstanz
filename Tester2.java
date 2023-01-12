package main;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Tester2 {
    // executes Experiment 2
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        experiment2();
    }

    public static void experiment2() throws ExecutionException, InterruptedException {
        long start, end;
        int[] a;
        int maxP = 6;

        for (int i = 3; i <= 9; i++) {
            System.out.println("10^" + i);

            a = getDistinctRandomArray((int) Math.pow(10, i));
            start = System.currentTimeMillis();
            a = Sorter.mergeSort(a);
            end = System.currentTimeMillis();
            assert isSorted(a);
            System.out.println("MergeSort: " + (end - start));

            // for i == 9, parallel Mergesort runs out of memory
            if (i < 9) {
                a = getDistinctRandomArray((int) Math.pow(10, i));
                start = System.currentTimeMillis();
                a = Sorter.parallelMergeSort(a, maxP);
                end = System.currentTimeMillis();
                assert isSorted(a);
                System.out.println("Parallel MergeSort: " + (end - start));
            }

            a = getDistinctRandomArray((int) Math.pow(10, i));
            start = System.currentTimeMillis();
            a = Sorter.quickSort(a);
            end = System.currentTimeMillis();
            assert isSorted(a);
            System.out.println("QuickSort: " + (end - start));

            a = getDistinctRandomArray((int) Math.pow(10, i));
            start = System.currentTimeMillis();
            a = Sorter.parallelQuickSort(a, maxP, true);
            end = System.currentTimeMillis();
            assert isSorted(a);
            System.out.println("Parallel QuickSort: " + (end - start));

            System.out.println();
        }
    }

    /**
     * creates an array with numbers 0 to n - 1 which are randomly permuted
     * @param n
     * @return the random array
     */
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

    /**
     *
     * @param a
     * @return whether the array is sorted
     */
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
