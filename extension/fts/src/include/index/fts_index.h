#pragma once

#include "function/fts_config.h"
#include "index/fts_internal_table_info.h"
#include "storage/index/index.h"

namespace kuzu {
namespace fts_extension {

struct FTSInsertState;
struct FTSDeleteState;
struct TermInfo;

struct FTSStorageInfo final : storage::IndexStorageInfo {
    common::idx_t numDocs = 0;
    double avgDocLen = 0;
    common::offset_t numCheckpointedNodes;

    FTSStorageInfo(common::idx_t numDocs, double avgDocLen, common::offset_t numCheckpointedNodes)
        : numDocs{numDocs}, avgDocLen{avgDocLen}, numCheckpointedNodes{numCheckpointedNodes} {}

    std::shared_ptr<common::BufferWriter> serialize() const override;

    static std::unique_ptr<IndexStorageInfo> deserialize(
        std::unique_ptr<common::BufferReader> reader);
};

class FTSIndex final : public storage::Index {
public:
    FTSIndex(storage::IndexInfo indexInfo, std::unique_ptr<storage::IndexStorageInfo> storageInfo,
        FTSConfig ftsConfig, main::ClientContext* context);

    static std::unique_ptr<Index> load(main::ClientContext* context,
        storage::StorageManager* storageManager, storage::IndexInfo indexInfo,
        std::span<uint8_t> storageInfoBuffer);

    std::unique_ptr<InsertState> initInsertState(main::ClientContext*,
        storage::visible_func isVisible) override;

    void insert(transaction::Transaction* transaction, const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& indexVectors, InsertState& insertState) override;

    std::unique_ptr<UpdateState> initUpdateState(main::ClientContext* context,
        common::column_id_t columnID, storage::visible_func isVisible) override;

    void update(transaction::Transaction* transaction, const common::ValueVector& nodeIDVector,
        common::ValueVector& propertyVector, UpdateState& updateState) override;

    std::unique_ptr<DeleteState> initDeleteState(const transaction::Transaction* transaction,
        storage::MemoryManager* mm, storage::visible_func isVisible) override;

    void delete_(transaction::Transaction* transaction, const common::ValueVector& nodeIDVector,
        DeleteState& deleteState) override;

    void finalize(main::ClientContext* context) override;
    void checkpoint(main::ClientContext*, storage::PageAllocator& pageAllocator) override;

    static storage::IndexType getIndexType() {
        static const storage::IndexType FTS_INDEX_TYPE{"FTS",
            storage::IndexConstraintType::SECONDARY_NON_UNIQUE,
            storage::IndexDefinitionType::EXTENSION, load};
        return FTS_INDEX_TYPE;
    }
    FTSInternalTableInfo& getInternalTableInfo() { return internalTableInfo; }

private:
    common::nodeID_t insertToDocTable(transaction::Transaction* transaction,
        FTSInsertState& ftsInsertState, common::nodeID_t insertedNodeID, uint64_t docLen) const;

    void insertToTermsTable(transaction::Transaction* transaction,
        std::unordered_map<std::string, TermInfo>& tfCollection,
        FTSInsertState& ftsInsertState) const;

    void insertToAppearsInTable(transaction::Transaction* transaction,
        const std::unordered_map<std::string, TermInfo>& tfCollection,
        FTSInsertState& ftsInsertState, common::nodeID_t docID,
        common::table_id_t termsTableID) const;

    common::nodeID_t deleteFromDocTable(transaction::Transaction* transaction,
        FTSDeleteState& deleteState, common::nodeID_t deletedNodeID, double& totalDocLen) const;

    void deleteFromTermsTable(transaction::Transaction* transaction,
        std::unordered_map<std::string, TermInfo>& tfCollection,
        FTSDeleteState& ftsDeleteState) const;

    void deleteFromAppearsInTable(transaction::Transaction* transaction,
        FTSDeleteState& ftsDeleteState, common::nodeID_t docID) const;

private:
    FTSInternalTableInfo internalTableInfo;
    FTSConfig config;
};

} // namespace fts_extension
} // namespace kuzu
