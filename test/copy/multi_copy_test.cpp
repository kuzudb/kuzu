#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>

#include "common/constants.h"
#include "common/file_system/local_file_system.h"
#include "common/string_format.h"
#include "graph_test/graph_test.h"

namespace kuzu {
namespace testing {

class MultiCopyTest : public EmptyDBTest {
public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
        auto result = conn->query("CREATE NODE TABLE Test(id int32, primary key(id))");
        ASSERT_TRUE(result->isSuccess()) << result->toString();
        result =
            conn->query("CREATE NODE TABLE TestSerial(id serial, prop int32, primary key(id))");
        ASSERT_TRUE(result->isSuccess()) << result->toString();
    }

    std::string generateFile(size_t values) {
        auto tempDir = TestHelper::getTempDir(getTestGroupAndName());
        auto filePath = common::LocalFileSystem::joinPath(tempDir, "tmp.csv");
#if defined(_WIN32)
        std::replace(filePath.begin(), filePath.end(), '\\', '/');
#endif
        std::filesystem::create_directories(tempDir);
        std::ofstream file(filePath);
        for (size_t i = 0; i < values; i++) {
            file << (totalTuples + i) << std::endl;
        }
        file.close();
        totalTuples += values;
        return filePath;
    }

    void copy(size_t values) {
        auto filePath = generateFile(values);
        auto result = conn->query(common::stringFormat("COPY Test FROM '{}'", filePath));
        ASSERT_TRUE(result->isSuccess()) << result->toString();
    }

    void copySerial(size_t values) {
        auto filePath = generateFile(values);
        auto result =
            conn->query(common::stringFormat("COPY TestSerial FROM '{}' (HEADER=false)", filePath));
        ASSERT_TRUE(result->isSuccess()) << result->toString();
    }

    void validate(bool isSerial = false) const {
        std::unique_ptr<main::QueryResult> countResult;
        if (isSerial) {
            countResult = conn->query("MATCH (t:TestSerial) RETURN COUNT(*)");
        } else {
            countResult = conn->query("MATCH (t:Test) RETURN COUNT(*)");
        }
        ASSERT_TRUE(countResult->isSuccess()) << countResult->toString();
        ASSERT_EQ(countResult->getNumTuples(), 1);
        ASSERT_EQ(countResult->getNext()->getValue(0)->getValue<int64_t>(), totalTuples);

        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> dist(0, totalTuples - 1);
        // Sample and check up to 1000 random elements in the range
        if (isSerial) {
            for (size_t i = 0; i < std::min(static_cast<size_t>(1000), totalTuples); i++) {
                auto index = dist(rng);
                auto result = conn->query(common::stringFormat(
                    "MATCH (t:TestSerial) WHERE t.id = {} RETURN t.id", index));
                ASSERT_TRUE(result->isSuccess()) << result->toString();
                ASSERT_EQ(result->getNumTuples(), 1) << "ID " << index << " is missing";
            }
        } else {
            for (size_t i = 0; i < std::min(static_cast<size_t>(1000), totalTuples); i++) {
                auto index = dist(rng);
                auto result = conn->query(
                    common::stringFormat("MATCH (t:Test) WHERE t.id = {} RETURN t.id", index));
                ASSERT_TRUE(result->isSuccess()) << result->toString();
                ASSERT_EQ(result->getNumTuples(), 1) << "ID " << index << " is missing";
                ASSERT_EQ(result->getNext()->getValue(0)->getValue<int32_t>(), index);
            }
        }
    }

private:
    size_t totalTuples = 0;
};

TEST_F(MultiCopyTest, SingleSerialCopy) {
    copySerial(common::StorageConstants::NODE_GROUP_SIZE * 10.5);
    validate(true /* isSerial */);
}

// Tests that multiple copies that only add to the existing node group succeed
// This is also covered by the tinysnb dataset
TEST_F(MultiCopyTest, OneNodeGroup) {
    copy(150);
    copy(175);
    copy(1);
    copy(5);
    validate();
}

// Tests that a second copy that does not modify any existing node groups succeeds
TEST_F(MultiCopyTest, FullFirstNodeGroup) {
    copy(common::StorageConstants::NODE_GROUP_SIZE);
    copy(common::StorageConstants::NODE_GROUP_SIZE);
    copy(common::StorageConstants::NODE_GROUP_SIZE);
    copy(common::StorageConstants::NODE_GROUP_SIZE);
    copy(common::StorageConstants::NODE_GROUP_SIZE * 1.5);
    validate();
}

// Tries to test that appendIncompleteNodeGroup works when it fills the shared node group but the
// shared node group needs to write to an existing node group The actual results may vary depending
// on how the work is divided between threads. This assumes that two test threads handle half of the
// work each, so that each thread finishes with appendIncompleteNodeGroup and the second one fills
// the shared group.
TEST_F(MultiCopyTest, PartialGroupSecondCopyWrite) {
    copy(common::StorageConstants::NODE_GROUP_SIZE / 2);
    copy(common::StorageConstants::NODE_GROUP_SIZE);
    validate();
}

// Tests that a second copy that modifies the existing node group and copies more than one node
// group succeeds
TEST_F(MultiCopyTest, PartialFirstNodeGroup) {
    copy(common::StorageConstants::NODE_GROUP_SIZE / 2);
    copy(common::StorageConstants::NODE_GROUP_SIZE * 2);
    copy(common::StorageConstants::NODE_GROUP_SIZE * 2);
    validate();
}

// Each thread should have ~3/4 of a node group in their
// local node group, so the second one to write that to the shared group
// will need to write twice before it can move its remaining nodes into the shared group
// See https://github.com/kuzudb/kuzu/issues/3714
TEST_F(MultiCopyTest, SharedWriteToExistingNodeGroup) {
    copy(common::StorageConstants::NODE_GROUP_SIZE * 0.75);
    copy(common::StorageConstants::NODE_GROUP_SIZE * systemConfig->maxNumThreads * 0.75);
    validate();
}

// Tests that a second copy that copies a large number of node groups succeeds
TEST_F(MultiCopyTest, MultipleNodeGroups) {
    copy(common::StorageConstants::NODE_GROUP_SIZE * 10.1);
    copy(common::StorageConstants::NODE_GROUP_SIZE * 20.8);
    copy(1);
    copy(common::StorageConstants::NODE_GROUP_SIZE * 3);
    validate();
}

} // namespace testing
} // namespace kuzu
