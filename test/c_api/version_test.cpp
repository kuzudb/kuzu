#include <filesystem>
#include <fstream>

#include "c_api/kuzu.h"
#include "c_api_test/c_api_test.h"
#include "gtest/gtest.h"

using namespace kuzu::main;
using namespace kuzu::testing;
using namespace kuzu::common;

class CApiVersionTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiVersionTest, GetVersion) {
    auto version = kuzu_get_version();
    ASSERT_NE(version, nullptr);
    ASSERT_STREQ(version, KUZU_CMAKE_VERSION);
    kuzu_destroy_string(version);
}

TEST_F(CApiVersionTest, GetStorageVersion) {
    auto storageVersion = kuzu_get_storage_version();
    if (inMemMode) {
        GTEST_SKIP();
    }
    // Reset the database to ensure that the lock on db file is released.
    conn.reset();
    database.reset();
    auto data = std::filesystem::path(databasePath);
    std::ifstream dbFile;
    dbFile.open(data, std::ios::binary);
    char magic[5];
    dbFile.read(magic, 4);
    magic[4] = '\0';
    ASSERT_STREQ(magic, "KUZU");
    uint64_t actualVersion;
    dbFile.read(reinterpret_cast<char*>(&actualVersion), sizeof(actualVersion));
    dbFile.close();
    ASSERT_EQ(storageVersion, actualVersion);
}

class EmptyCApiVersionTest : public CApiTest {
public:
    std::string getInputDir() override { return "empty"; }
};

TEST_F(EmptyCApiVersionTest, GetStorageVersion) {
    auto storageVersion = kuzu_get_storage_version();
    if (inMemMode) {
        GTEST_SKIP();
    }
    // Reset the database to ensure that the lock on db file is released.
    conn.reset();
    database.reset();
    auto data = std::filesystem::path(databasePath);
    std::ifstream dbFile;
    dbFile.open(data, std::ios::binary);
    char magic[5];
    dbFile.read(magic, 4);
    magic[4] = '\0';
    ASSERT_STREQ(magic, "KUZU");
    uint64_t actualVersion;
    dbFile.read(reinterpret_cast<char*>(&actualVersion), sizeof(actualVersion));
    dbFile.close();
    ASSERT_EQ(storageVersion, actualVersion);
}
