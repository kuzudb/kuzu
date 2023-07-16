#pragma once

#include "processor/operator/copy/copy_node.h"

namespace kuzu {
namespace processor {

struct CopyNPYNodeLocalState : public CopyNodeLocalState {
public:
    CopyNPYNodeLocalState(const common::CopyDescription& copyDesc, storage::NodeTable* table,
        storage::RelsStore* relsStore, catalog::Catalog* catalog, storage::WAL* wal,
        const DataPos& offsetVectorPos, const DataPos& columnIdxPos,
        std::vector<DataPos> arrowColumnPoses)
        : CopyNodeLocalState{copyDesc, table, relsStore, catalog, wal, offsetVectorPos,
              std::move(arrowColumnPoses)},
          columnIdxPos{columnIdxPos}, columnIdxVector{nullptr} {}

    DataPos columnIdxPos;
    common::ValueVector* columnIdxVector;
};

class CopyNPYNode : public CopyNode {
public:
    CopyNPYNode(std::unique_ptr<CopyNPYNodeLocalState> localState,
        std::shared_ptr<CopyNodeSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : CopyNode{std::move(localState), std::move(sharedState), std::move(resultSetDescriptor),
              std::move(child), id, paramsString} {}

    void executeInternal(ExecutionContext* context) final;

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final {
        auto npyLocalState = (CopyNPYNodeLocalState*)localState.get();
        npyLocalState->offsetVector =
            resultSet->getValueVector(npyLocalState->offsetVectorPos).get();
        npyLocalState->columnIdxVector =
            resultSet->getValueVector(npyLocalState->columnIdxPos).get();
        for (auto& arrowColumnPos : npyLocalState->arrowColumnPoses) {
            npyLocalState->arrowColumnVectors.push_back(
                resultSet->getValueVector(arrowColumnPos).get());
        }
    }

    void flushChunksAndPopulatePKIndexSingleColumn(
        std::vector<std::unique_ptr<storage::InMemColumnChunk>>& columnChunks,
        common::offset_t startNodeOffset, common::offset_t endNodeOffset,
        common::vector_idx_t columnToCopy);

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyNPYNode>(
            std::make_unique<CopyNPYNodeLocalState>((CopyNPYNodeLocalState&)*localState),
            sharedState, resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
