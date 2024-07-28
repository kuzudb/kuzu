#pragma once

#include <filesystem>

#include "common/string_format.h"
#include "gtest/gtest.h"
#include "main/kuzu.h"
#include "test_helper/test_helper.h"

using ::testing::Test;

namespace kuzu {
namespace testing {

static void removeDir(const std::string& dir) {
    if (std::filesystem::exists(dir)) {
        std::error_code removeErrorCode;
        if (!std::filesystem::remove_all(dir, removeErrorCode)) {
            throw common::Exception(common::stringFormat(
                "Error removing directory {}.  Error Message: {}", dir, removeErrorCode.message()));
        }
    }
}

class BaseGraphTest : public Test {
public:
    void SetUp() override {
        systemConfig = TestHelper::getSystemConfigFromEnv();
        setDatabasePath();
        removeDir(databasePath);

        ieDBPath = "";
    }

    virtual std::string getInputDir() = 0;

    void TearDown() override { removeDir(databasePath); }

    void createDBAndConn();

    // multiple conns test
    void createDB();
    virtual void createConns(const std::set<std::string>& connNames);

    void initGraph() { initGraph(getInputDir()); }
    void initGraph(std::string datasetDir);

    void setIEDatabasePath(std::string filePath) { ieDBPath = filePath; }
    void removeIEDBPath() const {
        if (ieDBPath != "") {
            auto lastSlashPos = ieDBPath.rfind('/');
            auto deletePath = ieDBPath.substr(0, lastSlashPos);
            removeDir(deletePath);
        }
    }

protected:
    // Static functions to access Database's non-public properties/interfaces.
    static storage::BufferManager* getBufferManager(main::Database& database) {
        return database.bufferManager.get();
    }
    static common::VirtualFileSystem* getFileSystem(main::Database& database) {
        return database.vfs.get();
    }

    // Static functions to access Connection's non-public properties/interfaces.
    static main::ClientContext* getClientContext(main::Connection& connection) {
        return connection.clientContext.get();
    }
    static void sortAndCheckTestResults(std::vector<std::string>& actualResult,
        std::vector<std::string>& expectedResult) {
        sort(expectedResult.begin(), expectedResult.end());
        ASSERT_EQ(actualResult, expectedResult);
    }

    std::string getTestGroupAndName() {
        const ::testing::TestInfo* const testInfo =
            ::testing::UnitTest::GetInstance()->current_test_info();
        return std::string(testInfo->test_case_name()) + "." + std::string(testInfo->name());
    }

private:
    void setDatabasePath() { databasePath = TestHelper::getTempDir(getTestGroupAndName()); }

public:
    std::string databasePath;
    std::unique_ptr<main::SystemConfig> systemConfig;
    std::unique_ptr<main::Database> database;
    // for normal conn; if it is null, represents multiple conns, need to use connMap
    std::unique_ptr<main::Connection> conn;
    // for multiple conns
    std::unordered_map<std::string, std::unique_ptr<main::Connection>> connMap;
    // for export import db
    std::string ieDBPath;
};

} // namespace testing
} // namespace kuzu
