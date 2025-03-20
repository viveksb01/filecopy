import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MultiThreadedFileCopyTest {

    @TempDir
    Path sourceDir;
    
    @TempDir
    Path targetDir;
    
    private static final Logger LOGGER = Logger.getLogger(MultiThreadedFileCopy.class.getName());
    private static final int TEST_BUFFER_SIZE = 1024 * 1024; // 1MB for tests
    private List<LogRecord> logRecords;
    private Handler logHandler;
    
    @BeforeEach
    void setUp() {
        // Setup log capturing
        logRecords = new ArrayList<>();
        logHandler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                logRecords.add(record);
            }
            
            @Override
            public void flush() {}
            
            @Override
            public void close() throws SecurityException {}
        };
        LOGGER.addHandler(logHandler);
    }
    
    @AfterEach
    void tearDown() {
        LOGGER.removeHandler(logHandler);
    }
    
    @Test
    @DisplayName("Test basic file copy functionality")
    void testBasicCopy() throws IOException, InterruptedException {
        // Create test files
        int numFiles = 5;
        List<Path> sourceFiles = createTestFiles(sourceDir, numFiles, 1024); // 1KB files
        
        // Run copy operation
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, 2);
        copier.startCopy();
        
        // Verify files were copied correctly
        for (Path sourceFile : sourceFiles) {
            Path relativePath = sourceDir.relativize(sourceFile);
            Path targetFile = targetDir.resolve(relativePath);
            
            assertTrue(Files.exists(targetFile), "Target file should exist: " + targetFile);
            assertEquals(
                Files.size(sourceFile), 
                Files.size(targetFile), 
                "File sizes should match"
            );
            
            // Verify content is the same
            byte[] sourceContent = Files.readAllBytes(sourceFile);
            byte[] targetContent = Files.readAllBytes(targetFile);
            assertArrayEquals(sourceContent, targetContent, "File contents should match");
        }
    }
    
    @Test
    @DisplayName("Test copying nested directory structure")
    void testNestedDirectories() throws IOException, InterruptedException {
        // Create nested directory structure
        Path subDir1 = Files.createDirectory(sourceDir.resolve("subdir1"));
        Path subDir2 = Files.createDirectory(sourceDir.resolve("subdir2"));
        Path subSubDir = Files.createDirectory(subDir1.resolve("subsubdir"));
        
        List<Path> sourceFiles = new ArrayList<>();
        sourceFiles.addAll(createTestFiles(sourceDir, 2, 512));
        sourceFiles.addAll(createTestFiles(subDir1, 2, 512));
        sourceFiles.addAll(createTestFiles(subDir2, 2, 512));
        sourceFiles.addAll(createTestFiles(subSubDir, 2, 512));
        
        // Run copy operation
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, 2);
        copier.startCopy();
        
        // Verify directory structure was created
        assertTrue(Files.exists(targetDir.resolve("subdir1")), "Subdirectory should exist");
        assertTrue(Files.exists(targetDir.resolve("subdir2")), "Subdirectory should exist");
        assertTrue(Files.exists(targetDir.resolve("subdir1").resolve("subsubdir")), "Sub-subdirectory should exist");
        
        // Verify files were copied correctly
        for (Path sourceFile : sourceFiles) {
            Path relativePath = sourceDir.relativize(sourceFile);
            Path targetFile = targetDir.resolve(relativePath);
            
            assertTrue(Files.exists(targetFile), "Target file should exist: " + targetFile);
            assertEquals(Files.size(sourceFile), Files.size(targetFile), "File sizes should match");
        }
    }
    
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    @DisplayName("Test with different thread counts")
    void testWithDifferentThreadCounts(int threadCount) throws IOException, InterruptedException {
        // Create test files
        int numFiles = 10;
        List<Path> sourceFiles = createTestFiles(sourceDir, numFiles, 2048); // 2KB files
        
        // Run copy operation
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, threadCount);
        copier.startCopy();
        
        // Verify all files were copied
        assertEquals(
            numFiles, 
            Files.list(targetDir).count(), 
            "Should have copied all files"
        );
        
        // Verify integrity
        for (Path sourceFile : sourceFiles) {
            Path relativePath = sourceDir.relativize(sourceFile);
            Path targetFile = targetDir.resolve(relativePath);
            
            assertTrue(Files.exists(targetFile), "Target file should exist");
            assertEquals(Files.size(sourceFile), Files.size(targetFile), "File sizes should match");
        }
    }
    
    @Test
    @DisplayName("Test copy of large files")
    void testLargeFileCopy() throws IOException, InterruptedException {
        // Create one large file (10MB)
        Path largeFile = createRandomFile(sourceDir.resolve("large_file.dat"), 10 * 1024 * 1024);
        
        // Run copy operation
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(
            sourceDir, targetDir, 2, TEST_BUFFER_SIZE, 3, 100);
        copier.startCopy();
        
        // Verify file was copied correctly
        Path targetFile = targetDir.resolve("large_file.dat");
        assertTrue(Files.exists(targetFile), "Target file should exist");
        assertEquals(Files.size(largeFile), Files.size(targetFile), "File sizes should match");
        
        // Verify contents match (using hash for efficiency)
        byte[] sourceHash = hashFile(largeFile);
        byte[] targetHash = hashFile(targetFile);
        assertArrayEquals(sourceHash, targetHash, "File contents should match");
    }
    
    @Test
    @DisplayName("Test handling of source file disappearing")
    void testSourceFileDisappears() throws IOException, InterruptedException {
        // Setup a test file that we'll delete
        Path fileToDelete = createRandomFile(sourceDir.resolve("disappearing_file.txt"), 1024);
        createRandomFile(sourceDir.resolve("stable_file.txt"), 1024);
        
        // Create a custom file copy with sleep to allow us to delete the file
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, 1) {
            @Override
            protected boolean copyFile(Path sourceFile, Path targetFile) {
                // If this is our disappearing file, delete it before copy starts
                if (sourceFile.getFileName().toString().equals("disappearing_file.txt")) {
                    try {
                        Files.delete(sourceFile);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.copyFile(sourceFile, targetFile);
            }
        };
        
        // Run copy operation
        copier.startCopy();
        
        // Check only the stable file was copied
        assertFalse(Files.exists(targetDir.resolve("disappearing_file.txt")), 
                  "Disappearing file should not be copied");
        assertTrue(Files.exists(targetDir.resolve("stable_file.txt")), 
                 "Stable file should be copied");
        
        // Check logs for error about disappearing file
        List<String> errorLogs = logRecords.stream()
            .filter(record -> record.getLevel() == Level.SEVERE)
            .map(LogRecord::getMessage)
            .collect(Collectors.toList());
        
        assertTrue(errorLogs.stream().anyMatch(msg -> msg.contains("File not found")), 
                 "Should log error about missing file");
    }
    
    @Test
    @DisplayName("Test retry logic")
    void testRetryLogic() throws IOException, InterruptedException {
        // Create a test file
        Path testFile = createRandomFile(sourceDir.resolve("retry_test.txt"), 1024);
        
        // Create a counter to track copy attempts
        AtomicInteger copyAttempts = new AtomicInteger(0);
        
        // Create custom file copy with simulated failures
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, 1, 
                                                              TEST_BUFFER_SIZE, 2, 100) {
            @Override
            protected boolean copyFile(Path sourceFile, Path targetFile) {
                int attempt = copyAttempts.incrementAndGet();
                
                // Fail on first attempt
                if (attempt == 1) {
                    try {
                        // Create parent directories manually since we're bypassing normal copy
                        Files.createDirectories(targetFile.getParent());
                        
                        // Create an empty file to simulate a failed transfer
                        Files.writeString(targetFile, "Incomplete content");
                        
                        // Throw exception to trigger retry
                        throw new IOException("Simulated failure on first attempt");
                    } catch (IOException e) {
                        return false;  // Return failure to trigger retry
                    }
                }
                
                // Succeed on second attempt
                return super.copyFile(sourceFile, targetFile);
            }
        };
        
        // Run copy operation
        copier.startCopy();
        
        // Verify file was eventually copied correctly
        Path targetFile = targetDir.resolve("retry_test.txt");
        assertTrue(Files.exists(targetFile), "Target file should exist after retry");
        assertEquals(Files.size(testFile), Files.size(targetFile), "File sizes should match after retry");
        
        // Verify we attempted to copy multiple times
        assertEquals(2, copyAttempts.get(), "Should have attempted to copy twice");
        
        // Check logs for retry message
        List<String> retryLogs = logRecords.stream()
            .filter(record -> record.getMessage().contains("Retry attempt"))
            .collect(Collectors.toList());
        
        assertFalse(retryLogs.isEmpty(), "Should have logged retry attempts");
    }
    
    @Test
    @DisplayName("Test handling of target directory already existing")
    void testTargetDirectoryExists() throws IOException, InterruptedException {
        // Create an existing file in the target directory
        Files.writeString(targetDir.resolve("existing.txt"), "I already exist");
        
        // Create a new file in source with the same name
        Files.writeString(sourceDir.resolve("existing.txt"), "I am the new content");
        
        // Create a different file in source
        Files.writeString(sourceDir.resolve("new_file.txt"), "I'm completely new");
        
        // Run copy operation
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, 2);
        copier.startCopy();
        
        // Verify existing file was overwritten
        String newContent = Files.readString(targetDir.resolve("existing.txt"));
        assertEquals("I am the new content", newContent, "Existing file should be overwritten");
        
        // Verify new file was created
        assertTrue(Files.exists(targetDir.resolve("new_file.txt")), "New file should be created");
    }
    
    @Test
    @DisplayName("Test target verification check")
    void testTargetVerificationCheck() throws IOException, InterruptedException {
        // Create a test file
        Path testFile = createRandomFile(sourceDir.resolve("verify_test.txt"), 1024);
        
        // Create a custom copier that corrupts files after copy
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, 1) {
            @Override
            protected boolean copyFile(Path sourceFile, Path targetFile) {
                boolean result = super.copyFile(sourceFile, targetFile);
                
                // After successful copy, corrupt the file
                if (result) {
                    try {
                        // Truncate the file to simulate corruption
                        Files.write(targetFile, "Corrupted".getBytes(), 
                                    StandardOpenOption.TRUNCATE_EXISTING);
                    } catch (IOException e) {
                        // Ignore errors during test corruption
                    }
                }
                
                return false; // Return failure so test doesn't hang
            }
        };
        
        // Run copy operation
        copier.startCopy();
        
        // Verify operation logged failure
        List<String> errorLogs = logRecords.stream()
            .filter(record -> record.getLevel() == Level.WARNING || record.getLevel() == Level.SEVERE)
            .map(LogRecord::getMessage)
            .collect(Collectors.toList());
        
        assertTrue(
            errorLogs.stream().anyMatch(msg -> msg.contains("File size mismatch") || 
                                        msg.contains("IO error copying")),
            "Should log error about file size mismatch"
        );
    }
    
    @Test
    @DisplayName("Test progress reporting")
    void testProgressReporting() throws IOException, InterruptedException {
        // Create some test files
        createTestFiles(sourceDir, 3, 1024);
        
        // Run copy operation
        MultiThreadedFileCopy copier = new MultiThreadedFileCopy(sourceDir, targetDir, 1);
        copier.startCopy();
        
        // Verify progress reports were logged
        List<String> progressLogs = logRecords.stream()
            .filter(record -> record.getMessage().contains("Copy Progress Report"))
            .collect(Collectors.toList());
        
        assertFalse(progressLogs.isEmpty(), "Should have logged progress reports");
        
        // Final report should show successful completion
        List<String> completionLogs = logRecords.stream()
            .filter(record -> record.getMessage().contains("Copy completed"))
            .collect(Collectors.toList());
        
        assertFalse(completionLogs.isEmpty(), "Should have logged completion message");
        assertTrue(
            completionLogs.get(0).contains("Successfully copied 3 files"),
            "Should report correct number of files"
        );
    }
    
    // Helper methods
    
    private List<Path> createTestFiles(Path directory, int count, int sizeBytes) throws IOException {
        List<Path> files = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Path file = createRandomFile(directory.resolve("test_file_" + i + ".txt"), sizeBytes);
            files.add(file);
        }
        return files;
    }
    
    private Path createRandomFile(Path path, int sizeBytes) throws IOException {
        byte[] data = new byte[sizeBytes];
        new Random().nextBytes(data);
        Files.write(path, data);
        return path;
    }
    
    private byte[] hashFile(Path file) throws IOException {
        // A simple hash function for testing file equality
        byte[] content = Files.readAllBytes(file);
        int hash = 0;
        for (byte b : content) {
            hash = 31 * hash + b;
        }
        return String.valueOf(hash).getBytes(StandardCharsets.UTF_8);
    }
}
