package main;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * inherits from DataInputStream but incorporates a limit so that the end of a data run can be recognized
 */
public class LimitedStream extends DataInputStream {
    private int limit = 0;
    private boolean hitLimit = false;
    private int blockSize = 0;
    private int blocksRead = 0;

    /**
     * Creates a DataInputStream that uses the specified
     * underlying InputStream.
     *
     * @param in the specified input stream
     */
    public LimitedStream(InputStream in) {
        super(in);
    }

    public void initialize(int limit, int blockSize) {
        this.limit = limit;
        this.blockSize = blockSize;
        blocksRead = 0;
        hitLimit = false;
    }

    /**
     * reads in one block of memory, using the respective block size, and stores it in array
     * @param array
     * @return
     * @throws IOException
     */
    public boolean readBlock(byte[] array) throws IOException {
        if (blocksRead >= limit) {
            hitLimit = true;
            return false;
        }
        try {
            readFully(array);
        } catch (EOFException e) {
            hitLimit = true;
            return false;
        }
        blocksRead++;
        return true;
    }

    public void resetCounter() {
        blocksRead = 0;
        hitLimit = false;
    }

    public boolean limitReached() {
        return hitLimit;
    }

    /**
     * skips over n blocks in the data stream
     * @param n
     * @return
     * @throws IOException
     */
    public boolean skipNBlocks(long n) throws IOException {
        try {
            skipNBytes(n * blockSize);
        } catch (EOFException e) {
            return false;
        }
        return true;
    }
}
