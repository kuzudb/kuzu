#include "main/storage_driver.h"
#include "main_test_helper/main_test_helper.h"

using namespace kuzu::testing;
using namespace kuzu::common;

TEST_F(ApiTest, StorageDriverScan) {
    auto storageDriver = std::make_unique<StorageDriver>(database.get());
    auto size = 3;
    auto nodeOffsetsBuffer = std::make_unique<uint8_t[]>(sizeof(offset_t) * size);
    auto nodeOffsets = (offset_t*)nodeOffsetsBuffer.get();
    nodeOffsets[0] = 7;
    nodeOffsets[1] = 0;
    nodeOffsets[2] = 3;
    auto result = storageDriver->scan("person", "ID", nodeOffsets, size);
    auto ids = (int64_t*)result.first.get();
    ASSERT_EQ(ids[0], 10);
    ASSERT_EQ(ids[1], 0);
    ASSERT_EQ(ids[2], 5);
}