#pragma once

#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/file_handle.h"

namespace graphflow {
namespace storage {

struct StorageStructureUtils {
    inline static void pinEachPageOfFile(FileHandle& fileHandle, BufferManager& bufferManager) {
        for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
            bufferManager.pin(fileHandle, pageIdx);
        }
    }

    inline static void unpinEachPageOfFile(FileHandle& fileHandle, BufferManager& bufferManager) {
        for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
            bufferManager.unpin(fileHandle, pageIdx);
        }
    }
};
} // namespace storage
} // namespace graphflow
