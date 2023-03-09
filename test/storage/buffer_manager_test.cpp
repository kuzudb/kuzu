#include "graph_test/graph_test.h"
#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class BufferManagerTests : public Test {

protected:
    void SetUp() override {
        FileUtils::createDir(TestHelper::getTmpTestDir());
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::STORAGE);
    }

    void TearDown() override {
        FileUtils::removeDir(TestHelper::getTmpTestDir());
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::STORAGE);
    }
};

TEST_F(BufferManagerTests, RemoveFilePagesFromFramesTest) {
    FileHandle fileHandle(std::string(TestHelper::getTmpTestDir()) + "bm_test.bin",
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    uint64_t numPagesToAdd = 1000;
    for (int pageIdx = 0; pageIdx < numPagesToAdd; ++pageIdx) {
        fileHandle.addNewPage();
    }
    auto bufferManager =
        std::make_unique<BufferManager>(StorageConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING *
                                            StorageConstants::DEFAULT_PAGES_BUFFER_RATIO,
            StorageConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING *
                StorageConstants::LARGE_PAGES_BUFFER_RATIO);
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
}
