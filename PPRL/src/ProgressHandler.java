package PPRL.src;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Threadsafe helper class for showing progress in terminal
 */
public class ProgressHandler {

    AtomicLong progressAbsolute;
    long totalSize;
    int stepPercent;
    int progressPercent;
    int lastMsgSize = 0;

    public ProgressHandler(long totalSize, int stepPercent) {
        this.progressAbsolute = new AtomicLong();
        this.totalSize = totalSize;
        this.stepPercent = stepPercent;
        this.progressPercent = 0;
    }

    public void updateProgress() {
        updateProgress(1);
    }

    public void updateProgress(int units) {
        for (int i = 0; i < units; i++) {
            progressAbsolute.incrementAndGet();
        }
        if ((100.0 * progressAbsolute.get() / totalSize) - progressPercent >= stepPercent) {
            progressPercent += stepPercent;
            printProgress();
        }
    }

    public void printProgress() {
        deleteLastMsg();
        String msg = progressPercent + "%";
        lastMsgSize = msg.length();
        System.out.print(msg);
    }

    private void deleteLastMsg() {
        for (int i = 0; i < lastMsgSize; i++) {
            System.out.print("\b");
        }
    }

    public void finish() {
        deleteLastMsg();
        System.out.println("Done.");
    }

    public void abort() {
        deleteLastMsg();
        System.out.println("Aborted");
    }

    public void reset() {
        this.progressAbsolute = new AtomicLong();
        this.progressPercent = 0;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }
}
