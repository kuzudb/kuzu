#pragma once

#include "storage/file_handle.h"

namespace kuzu {
namespace storage {

struct ShadowPageRecord {
    common::file_idx_t originalFileIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t originalPageIdx = common::INVALID_PAGE_IDX;

    void serialize(common::Serializer& serializer) const;
    static ShadowPageRecord deserialize(common::Deserializer& deserializer);
};

struct ShadowFileHeader {
    common::page_idx_t numShadowPages = 0;
};

// NOTE: This class is NOT thread-safe for now, as we are not checkpointing in parallel yet.
class ShadowFile {
public:
    ShadowFile(const std::string& directory, bool readOnly, BufferManager& bufferManager,
        common::VirtualFileSystem* vfs, main::ClientContext* context);

    DELETE_COPY_DEFAULT_MOVE(ShadowFile);

    bool hasShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage) const {
        return shadowPagesMap.contains(originalFile) &&
               shadowPagesMap.at(originalFile).contains(originalPage);
    }
    void clearShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage);
    common::page_idx_t getShadowPage(common::file_idx_t originalFile,
        common::page_idx_t originalPage) const;
    common::page_idx_t getOrCreateShadowPage(common::file_idx_t originalFile,
        common::page_idx_t originalPage);

    FileHandle& getShadowingFH() const { return *shadowingFH; }

    void replayShadowPageRecords(main::ClientContext& context) const;

    void flushAll() const;
    void clearAll(main::ClientContext& context);

private:
    FileHandle* shadowingFH;
    // The map caches shadow page idxes for pages in original files.
    std::unordered_map<common::file_idx_t,
        std::unordered_map<common::page_idx_t, common::page_idx_t>>
        shadowPagesMap;
    std::vector<ShadowPageRecord> shadowPageRecords;
};

} // namespace storage
} // namespace kuzu
