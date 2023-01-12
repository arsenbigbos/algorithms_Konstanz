package main;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * contains the different sorting algorithms
 */
public class Sorter {
    private static int threshold = 20000;
    public static int[] parallelQuickSort(int[] a, int p) throws ExecutionException, InterruptedException {
        return parallelQuickSort(a, p, false);
    }

    public static int[] parallelQuickSort(int[] a, int p, boolean moreP) throws InterruptedException, ExecutionException {
        ExecutorService executor = new ForkJoinPool(p);
        int[] buf = new int[a.length];
        parallelQuickSort(a, p, 0, a.length - 1, buf, executor, moreP);
        executor.shutdown();
        return a;
    }

    /**
     * Sorts the array using parallel QuickSort
     * @param a array to sort
     * @param p the number of cores
     * @param start where the subarray starts (inclusive)
     * @param end where the subarray ends (inclusive)
     * @param buf buffer array
     * @param executor Executerservice for parallelization
     * @param moreP whether the parallel partition method should be used
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static Void parallelQuickSort(int[] a, int p, int start, int end, int[] buf, ExecutorService executor, boolean moreP) throws InterruptedException, ExecutionException {
        if (start >= end) {
            return null;
        }

        int pivotIndex = partitionP(a, p, start, end, buf, executor, moreP);

        Future<Void> future = executor.submit(() -> parallelQuickSort(a, p, start, pivotIndex - 1, buf, executor, moreP));
        parallelQuickSort(a, p, pivotIndex + 1, end, buf, executor, moreP);
        future.get();
        return null;
    }

    /**
     * partitions the array into two subarrays in parallel
     * @param a array to sort
     * @param p the number of cores
     * @param start where the subarray starts (inclusive)
     * @param end where the subarray ends (inclusive)
     * @param buf buffer array
     * @param executor Executerservice for parallelization
     * @param moreP whether the parallel partition method should be used
     * @return index of the pivot element
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private static int partitionP(int[] a, int p, int start, int end, int[] buf, ExecutorService executor, boolean moreP) throws InterruptedException, ExecutionException {
        // if the size is below the threshold, use sequential partitioning for better performance
        if (!moreP || end - start + 1 < threshold) {
            return partition(a, start, end);
        }
        List<Callable<int[]>> tasks = new ArrayList<>(p);
        int subArraySize = (int) Math.ceil((double) (end - start + 1) / p);

        for (int i = start; i <= end; i += subArraySize) {
            int finalI = i;
            tasks.add(() -> partitionAndCount(a, finalI, Math.min(finalI + subArraySize - 1, end - 1), a[end]));
        }

        List<Future<int[]>> results = executor.invokeAll(tasks);
        tasks.clear();

        int offset = start;
        for (int i = 0; i < results.size(); i++) {
            int finalI = i;
            int finalOffset = offset;
            tasks.add(() -> {System.arraycopy(a, start + finalI * subArraySize, buf, finalOffset, results.get(finalI).get()[0]); return null;});
            offset += results.get(i).get()[0];
        }

        int pivotIndex = offset;
        buf[pivotIndex] = a[end];
        offset++;

        for (int i = 0; i < results.size(); i++) {
            int finalI = i;
            int finalOffset1 = offset;
            tasks.add(() -> {System.arraycopy(a, start + finalI * subArraySize + results.get(finalI).get()[0], buf, finalOffset1, results.get(finalI).get()[1]); return null;});
            offset += results.get(i).get()[1];
        }

        executor.invokeAll(tasks);

        System.arraycopy(buf, start, a, start, end - start + 1);
        return pivotIndex;
    }

    /**
     * partitions the subarray according to the given pivot element and returns the number of elements smaller
     * and greater than the pivot
     * @param a
     * @param start where the subarray starts (inclusive)
     * @param end where the subarray ends (inclusive)
     * @param pivot
     * @return an array containing [number of elements smaller, number of elements greater]
     */
    private static int[] partitionAndCount(int[] a, int start, int end, int pivot) {
        int i = start;
        int j = end;

        while (i <= j) {
            while (i <= j && a[i] <= pivot) {
                i++;
            }
            while (i <= j && a[j] >= pivot) {
                j--;
            }

            if (i <= j) {
                swap(a, i, j);
            }
        }
        int nLower = i - start;
        return new int[] {nLower, end - start + 1 - nLower};
    }

    public static int[] parallelMergeSort(int[] a, int p) throws InterruptedException {
        return parallelMergeSort(a, p, false);
    }

    public static int[] parallelMergeSort(int[] a, int p, boolean moreP) throws InterruptedException {
        return parallelMergeSort(a, p, a.length, moreP);
    }

    /**
     * sorts the array using parallel MergeSort
     * @param a array to sort
     * @param p number of cores
     * @param n
     * @param moreP whether parallel merge should be used
     * @return
     * @throws InterruptedException
     */
    public static int[] parallelMergeSort(int[] a, int p, int n, boolean moreP) throws InterruptedException {

        int subArraySize;
        int start;
        int[] buf = new int[a.length];

        ExecutorService executor = new ForkJoinPool(p);
        List<Callable<Void>> tasks = new ArrayList<>(n / 2);

        for (subArraySize = 1; subArraySize <= n - 1; subArraySize *= 2) {
            for (start = 0; start <= n - 1; start += 2 * subArraySize) {
                int mid = Math.min(start + subArraySize - 1, n - 1);
                int end = Math.min(start + 2 * subArraySize - 1, n - 1);

                int[] finalA = a;
                int[] finalBuf = buf;
                int finalStart = start;
                // if there aren't enough cores per subarray, use sequential merge instead for better performance
                if (!moreP || p / Math.ceil((double) n / (2 * subArraySize)) < 2) {
                    tasks.add(() -> {merge(finalA, finalBuf, finalStart, mid, end); return null;});
                } else {
                    tasks.add(() -> mergeP(finalA, finalBuf, finalStart, mid, end, executor));
                }
            }
            executor.invokeAll(tasks);
            tasks.clear();

            int[] tmp = a;
            a = buf;
            buf = tmp;
        }

        executor.shutdown();

        return a;
    }

    /**
     * merge the two subarrays in parallel
     * @param a source array
     * @param buf buffer array
     * @param leftStart (inclusive)
     * @param leftEnd (inclusive)
     * @param rightEnd (inclusive)
     * @param executor ExecutorService for parallelization
     * @return
     * @throws InterruptedException
     */
    private static Void mergeP(int[] a, int[] buf, int leftStart, int leftEnd, int rightEnd, ExecutorService executor)
            throws InterruptedException {
        if (2 * (leftEnd - leftStart + 1) < threshold) {
            merge(a, buf, leftStart, leftEnd, rightEnd);
            return null;
        }

        List<Callable<Void>> tasks = new ArrayList<>(rightEnd - leftStart + 1);
        for (int i = leftStart; i <= rightEnd; i++) {
            int finalI = i;
            tasks.add(() -> writeToCorrectPosition(a, buf, leftStart, leftEnd, rightEnd, finalI));
        }
        executor.invokeAll(tasks);
        return null;
    }

    /**
     * computes the correct position of the given element inside the resulting subarray by computing its position
     * inside the other subarray and taking its current position into account
     * @param a
     * @param buf
     * @param leftStart (inclusive)
     * @param leftEnd (inclusive)
     * @param rightEnd (inclusive)
     * @param currentPos
     * @return
     */
    private static Void writeToCorrectPosition(int[] a, int[] buf, int leftStart, int leftEnd, int rightEnd,
                                               int currentPos) {
        int posInSubArr;
        int start;
        int end;
        if (currentPos <= leftEnd) {
            posInSubArr =  currentPos - leftStart;
            start = leftEnd + 1;
            end = rightEnd;
        } else {
            posInSubArr = currentPos - (leftEnd + 1);
            start = leftStart;
            end = leftEnd;
        }

        int i = binarySearch(a, a[currentPos], start, end);

        buf[posInSubArr + i - start + leftStart] = a[currentPos];

        return null;
    }

    /**
     * looks for where the element would be put inside the given subarray
     * @param a
     * @param element the element to insert
     * @param start (inclusive)
     * @param end (inclusive)
     * @return the index, where the element would be inserted
     */
    private static int binarySearch(int[] a, int element, int start, int end) {

        int i;
        while (start < end) {
            i = (start + end) / 2;
            if (element < a[i]) {
                end = i;
            } else {
                start = i + 1;
            }
        }

        return element < a[start] ? start : start + 1;
    }

    public static int[] quickSort(int[] a) {
        quickSort(a, 0, a.length - 1);
        return a;
    }

    /**
     * classical sequential QuickSort. After completion, the array a is sorted.
     * @param a
     * @param start
     * @param end
     */
    public static void quickSort(int[] a, int start, int end) {
        if (start >= end) {
            return;
        }

        int pivotIndex = partition(a, start, end);
        quickSort(a, start, pivotIndex - 1);
        quickSort(a, pivotIndex + 1, end);
    }

    /**
     * uses last element as pivot and positions it correctly. Every element smaller than the pivot ends up
     * on the left side, every element greater on the right side of the pivot
     * @param a
     * @param start (inclusive)
     * @param end (inclusive)
     * @return index of the pivot element
     */
    private static int partition(int[] a, int start, int end) {
        int i = start;
        int j = end - 1;

        while (i <= j) {
            while (i <= j && a[i] <= a[end]) {
                i++;
            }
            while (i <= j && a[j] >= a[end]) {
                j--;
            }

            if (i <= j) {
                swap(a, i, j);
            }
        }

        swap(a, i, end);
        return i;
    }

    public static void swap(int[] a, int i, int j) {
        int tmp = a[i];
        a[i] = a[j];
        a[j] = tmp;
    }

    public static int[] mergeSort(int[] a) {
        return mergeSort(a, a.length);
    }

    /**
     * classical sequential MergeSort
     * @param a
     * @param n
     * @return the sorted array
     */
    public static int[] mergeSort(int[] a, int n) {
        int subArraySize;
        int start;
        int[] buf = new int[a.length];

        for (subArraySize = 1; subArraySize <= n - 1; subArraySize *= 2) {
            for (start = 0; start <= n - 1; start += 2 * subArraySize) {
                int mid = Math.min(start + subArraySize - 1, n - 1);
                int end = Math.min(start + 2 * subArraySize - 1, n - 1);
                merge(a, buf, start, mid, end);
            }
            int[] tmp = a;
            a = buf;
            buf = tmp;
        }

        return a;
    }

    private static void merge(int[] a, int[] buf, int leftStart, int leftEnd, int rightEnd) {
        int i, j, k;

        i = leftStart;
        j = leftEnd + 1;
        k = leftStart;
        while (i <= leftEnd && j <= rightEnd) {
            if (a[i] <= a[j]) {
                buf[k] = a[i];
                i++;
            } else {
                buf[k] = a[j];
                j++;
            }
            k++;
        }

        while (i <= leftEnd) {
            buf[k] = a[i];
            i++;
            k++;
        }

        while (j <= rightEnd) {
            buf[k] = a[j];
            j++;
            k++;
        }
    }
}
