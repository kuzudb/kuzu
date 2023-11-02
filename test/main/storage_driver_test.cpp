#include "main/storage_driver.h"
#include "main_test_helper/main_test_helper.h"

using namespace kuzu::main;
using namespace kuzu::testing;
using namespace kuzu::common;

TEST_F(ApiTest, StorageDriverScan) {
    auto storageDriver = std::make_unique<StorageDriver>(database.get());
    auto size = 6;
    auto nodeOffsetsBuffer = std::make_unique<uint8_t[]>(sizeof(offset_t) * size);
    auto nodeOffsets = (offset_t*)nodeOffsetsBuffer.get();
    nodeOffsets[0] = 7;
    nodeOffsets[1] = 0;
    nodeOffsets[2] = 3;
    nodeOffsets[3] = 1;
    nodeOffsets[4] = 2;
    nodeOffsets[5] = 6;
    auto result = std::make_unique<uint8_t[]>(sizeof(int64_t) * size);
    auto resultBuffer = (uint8_t*)result.get();
    storageDriver->scan("person", "ID", nodeOffsets, size, resultBuffer, 3);
    auto ids = (int64_t*)resultBuffer;
    ASSERT_EQ(ids[0], 10);
    ASSERT_EQ(ids[1], 0);
    ASSERT_EQ(ids[2], 5);
    ASSERT_EQ(ids[3], 2);
    ASSERT_EQ(ids[4], 3);
    ASSERT_EQ(ids[5], 9);
}
