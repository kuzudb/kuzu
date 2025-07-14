# File Descriptor Leak Tests for Kuzu Database

This document describes the comprehensive file descriptor leak testing framework for Kuzu database.

## Overview

Since Kuzu is a single-file database that creates temporary files for WAL, shadow files, spilling, and extensions, it's critical to ensure that file descriptors are properly managed and cleaned up. This test suite provides comprehensive coverage for detecting file descriptor leaks in various scenarios.

## Test Architecture

### Core Components

1. **FileDescriptorMonitor** - Cross-platform utility for monitoring file descriptors
2. **ScopedFileDescriptorMonitor** - RAII wrapper for automatic leak detection
3. **Test Suites** - Comprehensive test cases covering different scenarios

### Design Principles

- **Cross-platform compatibility** (Linux, macOS, Windows)
- **Non-intrusive monitoring** - No modification to existing Kuzu code
- **Comprehensive coverage** - Tests all file types and scenarios
- **Automatic cleanup verification** - Ensures reliable test results
- **Integration with GoogleTest** - Uses existing test infrastructure

## Test Suites

### 1. Basic Database Operations (`file_descriptor_leak_test.cpp`)

Tests file descriptor management during normal database operations:

- **BasicDatabaseLifecycle** - Create, use, and destroy database
- **MultipleDatabaseConnections** - Multiple connections to same database
- **ReadOnlyDatabaseAccess** - Read-only database access
- **DatabaseTransactions** - Transaction commit/rollback scenarios
- **LargeDataOperations** - Large data operations that trigger spilling
- **ExceptionHandling** - Query errors and exception scenarios
- **ForcedCheckpoint** - Forced checkpoint on database close
- **InMemoryDatabase** - In-memory database operations
- **CustomBufferPoolSize** - Custom buffer pool configurations

### 2. WAL and Shadow Files (`wal_shadow_file_leak_test.cpp`)

Tests file descriptor management for WAL and shadow files:

- **WALFileLifecycle** - WAL file creation and cleanup
- **WALFileRollback** - WAL file cleanup after rollback
- **ShadowFileCheckpoint** - Shadow file creation during checkpoint
- **MultipleTransactionsWAL** - Multiple transactions and WAL management
- **TransactionExceptions** - Transaction exceptions and cleanup
- **DatabaseRecovery** - Database recovery scenarios
- **LargeTransactionWAL** - Large transactions and WAL files
- **FailedCheckpointCleanup** - Cleanup after failed checkpoints
- **ConcurrentWALAccess** - Concurrent access to WAL files

### 3. Error Scenarios (`error_scenario_file_leak_test.cpp`)

Tests file descriptor management under error conditions:

- **DatabaseConstructorException** - Database constructor failures
- **ConnectionCreationFailure** - Connection creation failures
- **QueryExecutionExceptions** - Query execution errors
- **TransactionRollbackExceptions** - Transaction rollback errors
- **OutOfMemoryScenarios** - Memory pressure scenarios
- **DiskFullScenarios** - Disk space exhaustion
- **ConcurrentAccessErrors** - Concurrent access issues
- **ForcedDatabaseShutdown** - Forced shutdown scenarios
- **ExtensionLoadingErrors** - Extension loading failures
- **DatabaseCorruptionScenarios** - Database corruption handling

## Usage

### Running the Tests

```bash
# Build tests
mkdir build && cd build
cmake -DBUILD_TESTS=ON ..
make

# Run all file descriptor leak tests
./test/storage/file_descriptor_leak_test
./test/storage/wal_shadow_file_leak_test
./test/storage/error_scenario_file_leak_test

# Run specific test
./test/storage/file_descriptor_leak_test --gtest_filter="FileDescriptorLeakTest.BasicDatabaseLifecycle"
```

### Using the Monitoring Framework

```cpp
#include "file_descriptor_monitor.h"

TEST_F(MyTest, TestFileDescriptorLeaks) {
    ScopedFileDescriptorMonitor monitor("TestFileDescriptorLeaks");
    monitor.trackDatabaseFiles("/path/to/database");
    
    {
        Database database("/path/to/database");
        Connection conn(&database);
        
        // Perform database operations
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, PRIMARY KEY (name))");
        
        // Monitor automatically tracks file descriptors
    }
    
    // Monitor automatically checks for leaks in destructor
}
```

### Manual Monitoring

```cpp
FileDescriptorMonitor monitor;
monitor.trackDatabaseFile("/path/to/database");
monitor.trackWALFile("/path/to/database.wal");
monitor.trackShadowFile("/path/to/database.shadow");

auto beforeSnapshot = monitor.takeSnapshot();

// Perform operations

auto afterSnapshot = monitor.takeSnapshot();
auto differences = monitor.compareSnapshots(beforeSnapshot, afterSnapshot);

if (!differences.empty()) {
    for (const auto& diff : differences) {
        std::cerr << "File descriptor leak: " << diff << std::endl;
    }
}

auto cleanupIssues = monitor.checkFileCleanup();
if (!cleanupIssues.empty()) {
    for (const auto& issue : cleanupIssues) {
        std::cerr << "Cleanup issue: " << issue << std::endl;
    }
}
```

## Platform-Specific Behavior

### Linux/macOS
- Uses `/proc/self/fd` to enumerate open file descriptors
- Tracks symbolic links to identify open files
- Monitors file descriptor count changes

### Windows
- Uses Windows Handle API to enumerate open handles
- Tracks handle count changes
- Different file path handling for Windows paths

## File Types Tracked

1. **Main Database File** (`database.kz`) - Primary database file
2. **WAL Files** (`database.kz.wal`) - Write-ahead log files
3. **Shadow Files** (`database.kz.shadow`) - Checkpoint shadow files
4. **Temporary Files** (`*.tmp`) - Spilling temporary files
5. **Extension Files** - Extension-specific files

## Common Leak Scenarios Detected

1. **BufferManager FileHandle Leaks** - FileHandles not properly cleaned up
2. **WAL/Shadow File Persistence** - Temporary files not removed
3. **Extension File Leaks** - Extension files not cleaned up
4. **Spiller Temporary Files** - Temporary files left behind
5. **VFS File Handle Tracking** - File handles not properly closed
6. **Exception Cleanup Failures** - Cleanup failures during exceptions

## Debugging File Descriptor Leaks

### Enable Verbose Logging
```cpp
// In test code
std::cout << "Current FD count: " << FileDescriptorMonitor::getCurrentFDCount() << std::endl;
auto openFiles = FileDescriptorMonitor::getOpenFiles();
for (const auto& file : openFiles) {
    std::cout << "Open file: " << file << std::endl;
}
```

### Check System Limits
```bash
# Check file descriptor limits
ulimit -n

# Monitor file descriptors in real-time
lsof -p $(pgrep -f kuzu_test)
```

### Platform-Specific Debugging

**Linux:**
```bash
# Check open file descriptors
ls -la /proc/$(pgrep -f kuzu_test)/fd/

# Monitor file descriptor usage
watch -n 1 "ls /proc/$(pgrep -f kuzu_test)/fd/ | wc -l"
```

**macOS:**
```bash
# Check open files
lsof -p $(pgrep -f kuzu_test)

# Monitor file handle count
fs_usage -f filesys -p $(pgrep -f kuzu_test)
```

**Windows:**
```cmd
# Monitor handle count
handle.exe -p kuzu_test.exe
```

## Contributing

When adding new tests:

1. Use `ScopedFileDescriptorMonitor` for automatic leak detection
2. Track all relevant database files using `trackDatabaseFiles()`
3. Test both success and failure scenarios
4. Ensure tests clean up properly in teardown
5. Add platform-specific considerations if needed

## Troubleshooting

### Common Issues

1. **False Positives**: Some system operations may create temporary file descriptors
2. **Platform Differences**: Different behavior on Windows vs Unix systems
3. **Timing Issues**: File descriptor cleanup may be asynchronous
4. **Test Isolation**: Tests may interfere with each other if not properly isolated

### Solutions

1. Use unique database paths for each test
2. Clean up test files in setup and teardown
3. Allow for small fluctuations in FD counts
4. Use proper synchronization for multi-threaded tests

## Future Improvements

1. **Real-time Monitoring** - Monitor file descriptors during test execution
2. **Memory Leak Detection** - Integrate with memory leak detection tools
3. **Performance Impact** - Measure performance impact of file operations
4. **Automated Leak Detection** - Integrate with CI/CD pipeline
5. **Extension Testing** - More comprehensive extension file testing