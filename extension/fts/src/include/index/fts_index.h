#pragma once

#include "function/fts_config.h"
#include "storage/index/index.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"

namespace kuzu {
namespace fts_extension {

class FTSIndex final : public storage::Index {
public:
    FTSIndex(storage::IndexInfo indexInfo, std::unique_ptr<storage::IndexStorageInfo> storageInfo,
        FTSConfig ftsConfig, main::ClientContext* context);

    std::unique_ptr<InsertState> initInsertState(const transaction::Transaction* transaction,
        storage::MemoryManager* mm, storage::visible_func isVisible) override;

    void insert(main::ClientContext* context, const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& indexVectors, InsertState& insertState) override;

private:
    FTSConfig config;

    std::shared_ptr<common::DataChunkState> dataChunkState;
    // Doc table
    storage::NodeTable* docTable;
    common::ValueVector docNodeIDVector;
    common::ValueVector docPKVector;
    common::ValueVector lenVector;
    std::vector<common::ValueVector*> docProperties;
    // Terms table
    storage::NodeTable* termsTable;
    common::ValueVector termsNodeIDVector;
    common::ValueVector termsPKVector;
    common::ValueVector termsDFVector;
    std::vector<common::ValueVector*> termsProperties;
    common::column_id_t dfColumnID;
    // AppearsIn table
    storage::RelTable* appearsInTable;
    common::ValueVector appearsInSrcVector;
    common::ValueVector appearsInDstVector;
    common::ValueVector relIDVector;
    common::ValueVector tfVector;
    std::vector<common::ValueVector*> appearsInProperties;
};

} // namespace fts_extension
} // namespace kuzu
