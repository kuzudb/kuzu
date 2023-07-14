#pragma once

#include "processor/operator/copy/copy_node.h"

namespace kuzu {
namespace processor {

class CopyNPYNode : public CopyNode {
public:
    CopyNPYNode(std::shared_ptr<CopyNodeSharedState> sharedState, CopyNodeDataInfo copyNodeDataInfo,
        DataPos columnIdxPos, const common::CopyDescription& copyDesc, storage::NodeTable* table,
        storage::RelsStore* relsStore, catalog::Catalog* catalog, storage::WAL* wal,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : CopyNode{std::move(sharedState), std::move(copyNodeDataInfo), copyDesc, table, relsStore,
              catalog, wal, std::move(resultSetDescriptor), std::move(child), id, paramsString},
          columnIdxPos{columnIdxPos}, columnIdxVector{nullptr} {}

    void executeInternal(ExecutionContext* context) final;

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final {
        offsetVector = resultSet->getValueVector(copyNodeDataInfo.offsetVectorPos).get();
        columnIdxVector = resultSet->getValueVector(columnIdxPos).get();
        for (auto& arrowColumnPos : copyNodeDataInfo.arrowColumnPoses) {
            arrowColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
        }
    }

    void flushChunksAndPopulatePKIndexSingleColumn(
        std::vector<std::unique_ptr<storage::InMemColumnChunk>>& columnChunks,
        common::offset_t startNodeOffset, common::offset_t endNodeOffset,
        common::vector_idx_t columnToCopy);

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyNPYNode>(sharedState, copyNodeDataInfo, columnIdxPos, copyDesc,
            table, relsStore, catalog, wal, resultSetDescriptor->copy(), children[0]->clone(), id,
            paramsString);
    }

private:
    DataPos columnIdxPos;
    common::ValueVector* columnIdxVector;
};

} // namespace processor
} // namespace kuzu
