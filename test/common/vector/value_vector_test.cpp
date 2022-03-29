
#include "include/gtest/gtest.h"

#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/memory_manager.h"

using namespace graphflow::common;
using namespace std;

TEST(ValueVectorTests, TestDefaultHasNull) {
    auto bufferManager = make_unique<BufferManager>();
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());
    ValueVector valueVector(memoryManager.get(), DataType::INT64);
    shared_ptr<DataChunkState> dataChunkState =
        make_shared<DataChunkState>(DEFAULT_VECTOR_CAPACITY);
    valueVector.state = dataChunkState;
    valueVector.state->selectedSize = 3;
    for (int i = 0; i < 3; ++i) {
        ((uint64_t*)valueVector.values)[i] = i;
    }
    // Test that initially the vector does not contain any NULLs.
    EXPECT_TRUE(valueVector.hasNoNullsGuarantee());

    // Test that after an update of a value to non-NULL the vector still does not contain any NULLs.
    valueVector.setNull(2, false);
    EXPECT_TRUE(valueVector.hasNoNullsGuarantee());

    // Test that after an update of a value to NULL the vector may now contain NULLs.
    valueVector.setNull(4, true);
    EXPECT_FALSE(valueVector.hasNoNullsGuarantee());

    // Test that even if we revert the previous update of non-NULL the vector may still
    // contain  NULLs.
    valueVector.setNull(4, false);
    EXPECT_FALSE(valueVector.hasNoNullsGuarantee());
}
