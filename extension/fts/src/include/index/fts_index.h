#pragma once

#include "function/fts_config.h"
#include "storage/index/index.h"

namespace kuzu {
namespace fts_extension {

class FTSIndex final : public storage::Index {
public:
    FTSIndex(storage::IndexInfo indexInfo, std::unique_ptr<storage::IndexStorageInfo> storageInfo,
        FTSConfig ftsConfig);

    static std::unique_ptr<Index> load(main::ClientContext* context,
        storage::StorageManager* storageManager, storage::IndexInfo indexInfo,
        std::span<uint8_t> storageInfoBuffer);

    std::unique_ptr<InsertState> initInsertState(const transaction::Transaction* transaction,
        storage::MemoryManager* mm, storage::visible_func isVisible) override;

    void insert(transaction::Transaction* transaction, const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& indexVectors, InsertState& insertState) override;
    void commitInsert(transaction::Transaction*, const common::ValueVector&,
        const std::vector<common::ValueVector*>&, InsertState&) override {
        // DO NOTHING.
        // For FTS index, insertions are handled when the new tuples are inserted into the base
        // table being indexed.
    }

    static storage::IndexType getIndexType() {
        static const storage::IndexType FTS_INDEX_TYPE{"FTS",
            storage::IndexConstraintType::SECONDARY_NON_UNIQUE,
            storage::IndexDefinitionType::EXTENSION, load};
        return FTS_INDEX_TYPE;
    }

private:
    FTSConfig config;
};

} // namespace fts_extension
} // namespace kuzu
