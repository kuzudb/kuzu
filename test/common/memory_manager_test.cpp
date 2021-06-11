#include "include/gtest/gtest.h"

#include "src/common/include/memory_manager.h"

using namespace graphflow::common;
using namespace std;

TEST(MemoryManagerTests, OutOfMemoryTest) {
    MemoryManager memMgr(1024);
    auto block = memMgr.allocateBlock(1000);
    ASSERT_NE(block, nullptr);
    try {
        block = memMgr.allocateBlock(1000);
        ASSERT_EQ(block, nullptr);
    } catch (invalid_argument const& error) {
        ASSERT_EQ(string(error.what()), "Out of memory");
    } catch (...) { FAIL(); }
}
