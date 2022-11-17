#include "test/test_utility/include/test_helper.h"

#include "src/storage/buffer_manager/include/buffer_manager.h"

using namespace kuzu::testing;

class BufferManagerTests : public Test {

protected:
    void SetUp() override { FileUtils::createDir(TestHelper::TEMP_TEST_DIR); }

    void TearDown() override { FileUtils::removeDir(TestHelper::TEMP_TEST_DIR); }
};

TEST_F(BufferManagerTests, RemoveFilePagesFromFramesTest) {
    FileHandle fileHandle(string(TestHelper::TEMP_TEST_DIR) + "bm_test.bin",
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    uint64_t numPagesToAdd = 1000;
    for (int pageIdx = 0; pageIdx < numPagesToAdd; ++pageIdx) {
        fileHandle.addNewPage();
    }
    BufferManager* bufferManager =
        new BufferManager(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
    // Pin and unpin some pages
    bufferManager->pinWithoutReadingFromFile(fileHandle, 10);
    bufferManager->pinWithoutReadingFromFile(fileHandle, 999);
    for (int pageIdx = 0; pageIdx < numPagesToAdd; ++pageIdx) {
        if (pageIdx == 10 || pageIdx == 999) {
            ASSERT_TRUE(FileHandle::isAFrame(fileHandle.getFrameIdx(pageIdx)));
        } else {
            ASSERT_FALSE(FileHandle::isAFrame(fileHandle.getFrameIdx(pageIdx)));
        }
    }
    bufferManager->unpin(fileHandle, 10);
    bufferManager->unpin(fileHandle, 999);
    bufferManager->removeFilePagesFromFrames(fileHandle);
    for (int pageIdx = 0; pageIdx < numPagesToAdd; ++pageIdx) {
        ASSERT_FALSE(FileHandle::isAFrame(fileHandle.getFrameIdx(pageIdx)));
    }
    delete bufferManager;
}
