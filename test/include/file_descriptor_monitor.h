#pragma once

#include <cstdint>
#include <filesystem>
#include <set>
#include <string>
#include <vector>

namespace kuzu {
namespace testing {

/**
 * Cross-platform file descriptor monitoring utility for detecting file descriptor leaks.
 * Tracks open file descriptors and files created by the database.
 */
class FileDescriptorMonitor {
public:
    struct FileInfo {
        std::string path;
        std::string type; // "database", "wal", "shadow"
        bool isOpen;

        FileInfo(const std::string& p, const std::string& t, bool open = true)
            : path(p), type(t), isOpen(open) {}
    };

    struct Snapshot {
        std::set<int> openFDs;       // Unix file descriptors
        std::set<void*> openHandles; // Windows file handles
        std::vector<FileInfo> files; // All tracked files
        size_t totalFDs;

        Snapshot() : totalFDs(0) {}
    };

    FileDescriptorMonitor();
    ~FileDescriptorMonitor();

    // Take a snapshot of current file descriptors and files
    Snapshot takeSnapshot();

    // Compare two snapshots and report differences
    std::vector<std::string> compareSnapshots(const Snapshot& before, const Snapshot& after);

    // Track specific database files
    void trackDatabaseFile(const std::string& dbPath);
    void trackWALFile(const std::string& walPath);
    void trackShadowFile(const std::string& shadowPath);
    void trackTempFile(const std::string& tempPath);
    void trackExtensionFile(const std::string& extensionPath);

    // Utility methods
    static size_t getCurrentFDCount();
    static std::vector<std::string> getOpenFiles(const std::string& processName = "");

private:
    std::vector<FileInfo> trackedFiles;
    std::string testTempDir;

    // Platform-specific implementations
    std::set<int> getOpenFileDescriptors();
    std::set<void*> getOpenFileHandles();
    void cleanupTrackedFiles();
};

/**
 * RAII wrapper for file descriptor monitoring in tests.
 * Automatically takes before/after snapshots and reports leaks.
 */
class ScopedFileDescriptorMonitor {
public:
    explicit ScopedFileDescriptorMonitor(const std::string& testName);
    ~ScopedFileDescriptorMonitor();

    // Manual snapshot management
    void takeBeforeSnapshot();
    void takeAfterSnapshot();
    void checkForLeaks();

    // Track database-specific files
    void trackDatabaseFiles(const std::string& dbPath);

    FileDescriptorMonitor& getMonitor() { return monitor; }

private:
    FileDescriptorMonitor monitor;
    FileDescriptorMonitor::Snapshot beforeSnapshot;
    FileDescriptorMonitor::Snapshot afterSnapshot;
    std::string testName;
    bool hasBeforeSnapshot;
    bool hasAfterSnapshot;
};

} // namespace testing
} // namespace kuzu