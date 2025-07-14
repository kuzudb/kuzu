#include "file_descriptor_monitor.h"

#include <algorithm>
#include <sstream>

#include <gtest/gtest.h>

// Platform-specific includes
#ifdef _WIN32
#include <psapi.h>
#include <tlhelp32.h>
#include <windows.h>
#else
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace kuzu {
namespace testing {

FileDescriptorMonitor::FileDescriptorMonitor() {
    // Create a unique temporary directory for this test
    testTempDir = std::filesystem::temp_directory_path() /
                  ("kuzu_fd_test_" + std::to_string(std::time(nullptr)));
    std::filesystem::create_directories(testTempDir);
}

FileDescriptorMonitor::~FileDescriptorMonitor() {
    cleanupTrackedFiles();
    // Clean up test temp directory
    std::error_code ec;
    std::filesystem::remove_all(testTempDir, ec);
    // Ignore errors during cleanup
}

FileDescriptorMonitor::Snapshot FileDescriptorMonitor::takeSnapshot() {
    Snapshot snapshot;

#ifdef _WIN32
    snapshot.openHandles = getOpenFileHandles();
    snapshot.totalFDs = snapshot.openHandles.size();
#else
    snapshot.openFDs = getOpenFileDescriptors();
    snapshot.totalFDs = snapshot.openFDs.size();
#endif

    // Copy current file tracking info
    snapshot.files = trackedFiles;

    return snapshot;
}

std::vector<std::string> FileDescriptorMonitor::compareSnapshots(const Snapshot& before,
    const Snapshot& after) {
    std::vector<std::string> differences;

    // Check for FD count increase
    if (after.totalFDs > before.totalFDs) {
        std::ostringstream oss;
        oss << "File descriptor count increased from " << before.totalFDs << " to "
            << after.totalFDs << " (+" << (after.totalFDs - before.totalFDs) << ")";
        differences.push_back(oss.str());
    }

#ifdef _WIN32
    // Check for new handles
    std::set<void*> newHandles;
    std::set_difference(after.openHandles.begin(), after.openHandles.end(),
        before.openHandles.begin(), before.openHandles.end(),
        std::inserter(newHandles, newHandles.begin()));

    if (!newHandles.empty()) {
        std::ostringstream oss;
        oss << "New file handles detected: " << newHandles.size();
        differences.push_back(oss.str());
    }
#else
    // Check for new FDs
    std::set<int> newFDs;
    std::set_difference(after.openFDs.begin(), after.openFDs.end(), before.openFDs.begin(),
        before.openFDs.end(), std::inserter(newFDs, newFDs.begin()));

    if (!newFDs.empty()) {
        std::ostringstream oss;
        oss << "New file descriptors detected: ";
        for (auto it = newFDs.begin(); it != newFDs.end(); ++it) {
            if (it != newFDs.begin())
                oss << ", ";
            oss << *it;
        }
        differences.push_back(oss.str());
    }
#endif

    return differences;
}

void FileDescriptorMonitor::trackDatabaseFile(const std::string& dbPath) {
    trackedFiles.emplace_back(dbPath, "database");
}

void FileDescriptorMonitor::trackWALFile(const std::string& walPath) {
    trackedFiles.emplace_back(walPath, "wal");
}

void FileDescriptorMonitor::trackShadowFile(const std::string& shadowPath) {
    trackedFiles.emplace_back(shadowPath, "shadow");
}

void FileDescriptorMonitor::trackTempFile(const std::string& tempPath) {
    trackedFiles.emplace_back(tempPath, "temp");
}

void FileDescriptorMonitor::trackExtensionFile(const std::string& extensionPath) {
    trackedFiles.emplace_back(extensionPath, "extension");
}

size_t FileDescriptorMonitor::getCurrentFDCount() {
#ifdef _WIN32
    // Windows implementation - count open handles
    DWORD processId = GetCurrentProcessId();
    HANDLE process = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, processId);
    if (!process)
        return 0;

    DWORD handleCount = 0;
    GetProcessHandleCount(process, &handleCount);
    CloseHandle(process);
    return handleCount;
#else
    // Unix implementation - count open file descriptors
    std::string fdDir = "/proc/self/fd";
    DIR* dir = opendir(fdDir.c_str());
    if (!dir)
        return 0;

    size_t count = 0;
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] != '.') {
            count++;
        }
    }
    closedir(dir);
    return count;
#endif
}

std::vector<std::string> FileDescriptorMonitor::getOpenFiles(const std::string& processName) {
    std::vector<std::string> openFiles;

#ifdef _WIN32
    // Windows implementation - enumerate open files
    // This is more complex on Windows, simplified for now
    return openFiles;
#else
    // Unix implementation - read /proc/self/fd
    std::string fdDir = "/proc/self/fd";
    DIR* dir = opendir(fdDir.c_str());
    if (!dir)
        return openFiles;

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] != '.') {
            std::string linkPath = fdDir + "/" + entry->d_name;
            char target[PATH_MAX];
            ssize_t len = readlink(linkPath.c_str(), target, sizeof(target) - 1);
            if (len > 0) {
                target[len] = '\0';
                openFiles.push_back(target);
            }
        }
    }
    closedir(dir);
#endif

    return openFiles;
}

std::set<int> FileDescriptorMonitor::getOpenFileDescriptors() {
    std::set<int> fds;

#ifndef _WIN32
    std::string fdDir = "/proc/self/fd";
    DIR* dir = opendir(fdDir.c_str());
    if (!dir)
        return fds;

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] != '.') {
            int fd = std::atoi(entry->d_name);
            if (fd > 0) {
                fds.insert(fd);
            }
        }
    }
    closedir(dir);
#endif

    return fds;
}

std::set<void*> FileDescriptorMonitor::getOpenFileHandles() {
    std::set<void*> handles;

#ifdef _WIN32
    // Windows implementation - this is complex and would require
    // additional libraries or system calls
    // For now, return empty set
#endif

    return handles;
}

void FileDescriptorMonitor::cleanupTrackedFiles() {
    for (auto& file : trackedFiles) {
        if (std::filesystem::exists(file.path)) {
            std::error_code ec;
            std::filesystem::remove(file.path, ec);
            // Mark as cleaned up
            file.isOpen = false;
        }
    }
}

// ScopedFileDescriptorMonitor implementation

ScopedFileDescriptorMonitor::ScopedFileDescriptorMonitor(const std::string& testName)
    : testName(testName), hasBeforeSnapshot(false), hasAfterSnapshot(false) {
    takeBeforeSnapshot();
}

ScopedFileDescriptorMonitor::~ScopedFileDescriptorMonitor() {
    if (!hasAfterSnapshot) {
        takeAfterSnapshot();
    }
    checkForLeaks();
}

void ScopedFileDescriptorMonitor::takeBeforeSnapshot() {
    beforeSnapshot = monitor.takeSnapshot();
    hasBeforeSnapshot = true;
}

void ScopedFileDescriptorMonitor::takeAfterSnapshot() {
    afterSnapshot = monitor.takeSnapshot();
    hasAfterSnapshot = true;
}

void ScopedFileDescriptorMonitor::checkForLeaks() {
    if (!hasBeforeSnapshot || !hasAfterSnapshot) {
        return;
    }

    // Check for FD leaks
    auto fdDifferences = monitor.compareSnapshots(beforeSnapshot, afterSnapshot);

    // Report issues
    if (!fdDifferences.empty()) {
        std::ostringstream oss;
        oss << "File descriptor leak detected in test: " << testName << "\n";

        for (const auto& diff : fdDifferences) {
            oss << "  FD Issue: " << diff << "\n";
        }

        // Use GoogleTest to report the failure
        ADD_FAILURE() << oss.str();
    }
}

void ScopedFileDescriptorMonitor::trackDatabaseFiles(const std::string& dbPath) {
    monitor.trackDatabaseFile(dbPath);
    monitor.trackWALFile(dbPath + ".wal");
    monitor.trackShadowFile(dbPath + ".shadow");

    // Track potential temporary files
    std::string tempDir = std::filesystem::temp_directory_path();
    std::string dbName = std::filesystem::path(dbPath).filename().string();
    monitor.trackTempFile(tempDir + "/" + dbName + ".tmp");
}

} // namespace testing
} // namespace kuzu