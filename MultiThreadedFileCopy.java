import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MultiThreadedFileCopy {
    private static final Logger LOGGER = Logger.getLogger(MultiThreadedFileCopy.class.getName());
    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024; // 8MB buffer size
    private static final int DEFAULT_RETRY_COUNT = 3;
    private static final int DEFAULT_RETRY_DELAY_MS = 1000;
    
    private final int numThreads;
    private final int bufferSize;
    private final int maxRetries;
    private final int retryDelayMs;
    private final ExecutorService executorService;
    private final Path sourceDir;
    private final Path targetDir;
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong totalBytesTransferred = new AtomicLong(0);
    private final ConcurrentHashMap<Path, TransferProgress> progressMap = new ConcurrentHashMap<>();
    
    public MultiThreadedFileCopy(Path sourceDir, Path targetDir, int numThreads) {
        this(sourceDir, targetDir, numThreads, DEFAULT_BUFFER_SIZE, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_DELAY_MS);
    }
    
    public MultiThreadedFileCopy(Path sourceDir, Path targetDir, int numThreads, 
                                int bufferSize, int maxRetries, int retryDelayMs) {
        this.sourceDir = sourceDir;
        this.targetDir = targetDir;
        this.numThreads = numThreads;
        this.bufferSize = bufferSize;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        this.executorService = Executors.newFixedThreadPool(numThreads);
        
        LOGGER.info(String.format("Initializing copy from %s to %s with %d threads", 
                    sourceDir, targetDir, numThreads));
    }
    
    public void startCopy() throws IOException, InterruptedException {
        // Ensure target directory exists
        Files.createDirectories(targetDir);
        
        // List all files to be copied
        List<Path> filesToCopy = listFiles(sourceDir);
        LOGGER.info("Found " + filesToCopy.size() + " files to copy");
        
        // Create progress monitor thread
        ScheduledExecutorService progressMonitor = Executors.newSingleThreadScheduledExecutor();
        progressMonitor.scheduleAtFixedRate(this::reportProgress, 1, 1, TimeUnit.SECONDS);
        
        // Submit copy tasks
        List<Future<Boolean>> futures = new ArrayList<>();
        for (Path sourceFile : filesToCopy) {
            Path relativePath = sourceDir.relativize(sourceFile);
            Path targetFile = targetDir.resolve(relativePath);
            
            // Create parent directories if they don't exist
            Files.createDirectories(targetFile.getParent());
            
            // Initialize progress tracker for this file
            long fileSize = Files.size(sourceFile);
            progressMap.put(sourceFile, new TransferProgress(sourceFile, targetFile, fileSize));
            
            // Submit copy task
            futures.add(executorService.submit(() -> copyFile(sourceFile, targetFile)));
        }
        
        // Wait for all tasks to complete
        for (Future<Boolean> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                LOGGER.log(Level.SEVERE, "Copy task failed", e);
                failureCount.incrementAndGet();
            }
        }
        
        // Shutdown executors
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        
        progressMonitor.shutdown();
        progressMonitor.awaitTermination(5, TimeUnit.SECONDS);
        
        // Final report
        LOGGER.info(String.format("Copy completed. Successfully copied %d files, failed %d files. Total bytes transferred: %d",
                    successCount.get(), failureCount.get(), totalBytesTransferred.get()));
    }
    
    private List<Path> listFiles(Path directory) throws IOException {
        List<Path> result = new ArrayList<>();
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!attrs.isDirectory()) {
                    result.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
            
            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                LOGGER.log(Level.WARNING, "Failed to access file: " + file, exc);
                return FileVisitResult.CONTINUE;
            }
        });
        return result;
    }
    
    private boolean copyFile(Path sourceFile, Path targetFile) {
        int retryCount = 0;
        boolean success = false;
        
        while (!success && retryCount <= maxRetries) {
            try {
                if (retryCount > 0) {
                    LOGGER.info(String.format("Retry attempt %d/%d for file %s", 
                                retryCount, maxRetries, sourceFile));
                    Thread.sleep(retryDelayMs);
                }
                
                // Check if source file still exists
                if (!Files.exists(sourceFile)) {
                    throw new FileNotFoundException("Source file no longer exists: " + sourceFile);
                }
                
                // Get file size
                long fileSize = Files.size(sourceFile);
                TransferProgress progress = progressMap.get(sourceFile);
                
                // Create temporary file for transfer
                Path tempFile = Files.createTempFile(targetFile.getParent(), "copy_", ".tmp");
                
                try (FileChannel sourceChannel = FileChannel.open(sourceFile, StandardOpenOption.READ);
                     FileChannel targetChannel = FileChannel.open(tempFile, StandardOpenOption.WRITE, 
                                                                StandardOpenOption.CREATE, 
                                                                StandardOpenOption.TRUNCATE_EXISTING)) {
                    
                    // Allocate direct buffer for better performance
                    ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
                    long bytesTransferred = 0;
                    int bytesRead;
                    
                    while ((bytesRead = sourceChannel.read(buffer)) != -1) {
                        // Flip buffer for writing
                        buffer.flip();
                        
                        // Write buffer to target file
                        while (buffer.hasRemaining()) {
                            int bytesWritten = targetChannel.write(buffer);
                            bytesTransferred += bytesWritten;
                            totalBytesTransferred.addAndGet(bytesWritten);
                            
                            // Update progress
                            if (progress != null) {
                                progress.updateProgress(bytesTransferred);
                            }
                        }
                        
                        // Clear buffer for next read
                        buffer.clear();
                    }
                    
                    // Force write to disk
                    targetChannel.force(true);
                }
                
                // Atomic rename of temp file to target file
                try {
                    Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, 
                              StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    // Fall back to non-atomic move if atomic move is not supported
                    Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
                }
                
                // Verify file sizes match
                long targetSize = Files.size(targetFile);
                if (targetSize != fileSize) {
                    throw new IOException(String.format(
                        "File size mismatch after copy. Source: %d bytes, Target: %d bytes", 
                        fileSize, targetSize));
                }
                
                // Mark as success
                success = true;
                successCount.incrementAndGet();
                LOGGER.info("Successfully copied: " + sourceFile + " to " + targetFile);
                
                // Remove from progress map after successful completion
                if (progress != null) {
                    progress.markComplete();
                }
                
            } catch (FileNotFoundException | NoSuchFileException e) {
                LOGGER.log(Level.SEVERE, "File not found: " + e.getMessage());
                failureCount.incrementAndGet();
                return false; // No need to retry if file is missing
                
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, String.format(
                    "IO error copying %s (attempt %d/%d): %s", 
                    sourceFile, retryCount + 1, maxRetries + 1, e.getMessage()));
                retryCount++;
                
                // If we've exhausted our retries, mark as failure
                if (retryCount > maxRetries) {
                    failureCount.incrementAndGet();
                    return false;
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Copy operation interrupted: " + sourceFile);
                failureCount.incrementAndGet();
                return false;
            }
        }
        
        return success;
    }
    
    private void reportProgress() {
        StringBuilder report = new StringBuilder("\n=== Copy Progress Report ===\n");
        report.append(String.format("Files: %d successful, %d failed, %d in progress\n", 
                    successCount.get(), failureCount.get(), 
                    progressMap.size() - successCount.get() - failureCount.get()));
        report.append(String.format("Total bytes transferred: %d\n", totalBytesTransferred.get()));
        
        // Report individual file progress for files in progress
        for (TransferProgress progress : progressMap.values()) {
            if (!progress.isComplete()) {
                report.append(progress.getProgressReport()).append("\n");
            }
        }
        
        LOGGER.info(report.toString());
    }
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java MultiThreadedFileCopy <source-dir> <target-dir> [num-threads] [buffer-size-MB]");
            System.exit(1);
        }
        
        Path sourceDir = Paths.get(args[0]);
        Path targetDir = Paths.get(args[1]);
        int numThreads = args.length > 2 ? Integer.parseInt(args[2]) : Runtime.getRuntime().availableProcessors();
        int bufferSizeMB = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_BUFFER_SIZE / (1024 * 1024);
        int bufferSize = bufferSizeMB * 1024 * 1024;
        
        try {
            MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, numThreads, 
                                                                    bufferSize, DEFAULT_RETRY_COUNT, 
                                                                    DEFAULT_RETRY_DELAY_MS);
            copier.startCopy();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Copy operation failed", e);
            System.exit(1);
        }
    }
    
    // Helper class to track and report progress for individual file transfer
    private static class TransferProgress {
        private final Path sourceFile;
        private final Path targetFile;
        private final long totalSize;
        private final AtomicLong bytesTransferred = new AtomicLong(0);
        private volatile boolean complete = false;
        private final long startTime = System.currentTimeMillis();
        
        TransferProgress(Path sourceFile, Path targetFile, long totalSize) {
            this.sourceFile = sourceFile;
            this.targetFile = targetFile;
            this.totalSize = totalSize;
        }
        
        void updateProgress(long bytesTransferred) {
            this.bytesTransferred.set(bytesTransferred);
        }
        
        void markComplete() {
            complete = true;
        }
        
        boolean isComplete() {
            return complete;
        }
        
        String getProgressReport() {
            long current = bytesTransferred.get();
            int percentage = totalSize > 0 ? (int)((current * 100) / totalSize) : 0;
            
            // Calculate transfer rate
            long elapsedSecs = (System.currentTimeMillis() - startTime) / 1000;
            double mbTransferred = current / (1024.0 * 1024.0);
            double transferRate = elapsedSecs > 0 ? mbTransferred / elapsedSecs : 0;
            
            return String.format("%s: %d%% (%d/%d bytes) - %.2f MB/s", 
                                sourceFile.getFileName(), percentage, current, totalSize, transferRate);
        }
    }
}
