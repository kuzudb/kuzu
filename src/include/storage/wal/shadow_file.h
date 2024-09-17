#pragma once

#include "function/hash/hash_functions.h"
#include "storage/db_file_id.h"
#include "storage/file_handle.h"

namespace kuzu {
namespace storage {

struct ShadowPageRecord {
    DBFileID dbFileID;
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

    bool hasShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage) const {
        return shadowPagesMap.contains(originalFile) &&
               shadowPagesMap.at(originalFile).contains(originalPage);
    }
    void clearShadowPage(common::file_idx_t originalFile, common::page_idx_t originalPage);
    common::page_idx_t getShadowPage(common::file_idx_t originalFile,
        common::page_idx_t originalPage) const;
    common::page_idx_t getOrCreateShadowPage(DBFileID dbFileID, common::file_idx_t originalFile,
        common::page_idx_t originalPage);

    FileHandle& getShadowingFH() const { return *shadowingFH; }

    void replayShadowPageRecords(main::ClientContext& context) const;

    void flushAll() const;
    void clearAll(main::ClientContext& context);

private:
    static std::unique_ptr<common::FileInfo> getFileInfo(const main::ClientContext& context,
        DBFileID dbFileID);

    void deserializeShadowPageRecords();

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

template<>
struct std::hash<kuzu::storage::DBFileID> {
    size_t operator()(const kuzu::storage::DBFileID& fileId) const noexcept {
        const auto dbFileTypeHash = std::hash<uint8_t>()(static_cast<uint8_t>(fileId.dbFileType));
        const auto isOverflowHash = std::hash<bool>()(fileId.isOverflow);
        const auto nodeIndexIDHash = std::hash<kuzu::common::table_id_t>()(fileId.tableID);
        return kuzu::function::combineHashScalar(dbFileTypeHash,
            kuzu::function::combineHashScalar(isOverflowHash, nodeIndexIDHash));
    }
};
