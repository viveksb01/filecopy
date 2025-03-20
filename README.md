# filecopy

This Java file copy utility addresses follwoing requirements:

Multi-threading:

Uses a configurable thread pool for parallel file transfers
Default thread count is based on available processors, but can be specified
Each file copy operation runs in its own thread


Memory Management:

Uses ByteBuffer.allocateDirect() for efficient I/O operations
Configurable buffer size (default 8MB)
Properly closes all resources using try-with-resources


Exception Handling:

Implements retry mechanism for network failures
Handles file not found, permission issues, and IO exceptions
Detailed logging of all errors


Corner Cases & Recovery:

Uses atomic file operations where possible
Creates temporary files during transfer to prevent corruption
Verifies file sizes after transfer
Handles cases where source file disappears
Creates target directories if they don't exist


Progress Reporting:

Shows per-file progress with percentage and transfer rate
Provides overall statistics (success count, failure count, total bytes)
Updates progress every second



The utility can be run from the command line with:
Copyjava MultiThreadedFileCopy <source-dir> <target-dir> [num-threads] [buffer-size-MB]
