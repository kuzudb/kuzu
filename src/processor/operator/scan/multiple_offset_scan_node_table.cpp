#include "processor/operator/scan/multiple_offset_scan_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void MultipleOffsetScanNodeTable::initLocalStateInternal(ResultSet* resultSet,
    kuzu::processor::ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    nodeTableInfo.localScanState = std::make_unique<NodeTableScanState>(nodeTableInfo.columnIDs);
    initVectors(*nodeTableInfo.localScanState, *resultSet);
}

bool MultipleOffsetScanNodeTable::getNextTuplesInternal(
    kuzu::processor::ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }

    auto transaction = context->clientContext->getTx();
    auto nodeIdVector = nodeTableInfo.localScanState->nodeIDVector;
    auto selSize = nodeIdVector->state->getSelVector().getSelSize();
    if (selSize == 0) {
        return false;
    }
    auto selPositions = nodeIdVector->state->getSelVector().getSelectedPositions();

    std::unordered_map<node_group_idx_t, std::vector<sel_t>> nodeGroupPositions;
    std::vector<sel_t> initialPositions;
    bool isUnfiltered = nodeIdVector->state->getSelVector().isUnfiltered();
    for (int i = 0; i < selSize; i++) {
        auto pos = selPositions[i];
        auto nodeId = nodeIdVector->getValue<nodeID_t>(pos);
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeId.offset);

        // Insert position into the correct node group
        if (!nodeGroupPositions.contains(nodeGroupIdx)) {
            nodeGroupPositions[nodeGroupIdx] = std::vector<sel_t>();
        }
        nodeGroupPositions[nodeGroupIdx].push_back(pos);
        initialPositions.push_back(pos);

        // Cache scan node information if not already cached
        if (!scanNodeInfoCache.contains(nodeGroupIdx)) {
            ScanNodeTableInfo copy = nodeTableInfo.copy();
            copy.localScanState = std::make_unique<NodeTableScanState>(copy.columnIDs);
            copy.localScanState->nodeGroupIdx = nodeGroupIdx;
            // init vectors
            initVectors(*copy.localScanState, *resultSet);
            copy.localScanState->source = TableScanSource::COMMITTED;
            copy.table->initializeScanState(transaction, *copy.localScanState);
            scanNodeInfoCache.emplace(nodeGroupIdx, std::move(copy));
        }
    }

    for (const auto& [nodeGroupIdx, positions] : nodeGroupPositions) {
        // nodeGroupIdx is the key (node group index)
        // positions is the vector of sel_t containing positions for this node group
        auto& localScanState = scanNodeInfoCache.at(nodeGroupIdx).localScanState;

        // Iterate over each position in the current node group
        for (const auto& pos : positions) {
            // Set the selection vector size to 1 for this single position
            localScanState->nodeIDVector->state->getSelVectorUnsafe().setSelSize(1);
            localScanState->nodeIDVector->state->getSelVectorUnsafe().setToFiltered();
            localScanState->nodeIDVector->state->getSelVectorUnsafe()[0] = pos;

            for (auto& outputVector : localScanState->outputVectors) {
                outputVector->state->getSelVectorUnsafe().setSelSize(1);
                outputVector->state->getSelVectorUnsafe().setToFiltered();
                outputVector->state->getSelVectorUnsafe()[0] = pos;
            }

            // Perform the lookup for this single position
            nodeTableInfo.table->lookup(transaction, *localScanState);
        }
    }

    // Reset sel vector based on initial positions
    auto& localScanState = nodeTableInfo.localScanState;
    localScanState->nodeIDVector->state->getSelVectorUnsafe().setSelSize(initialPositions.size());
    if (isUnfiltered) {
        localScanState->nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered();
    }
    for (auto& outputVector : localScanState->outputVectors) {
        outputVector->state->getSelVectorUnsafe().setSelSize(initialPositions.size());
        if (isUnfiltered) {
            outputVector->state->getSelVectorUnsafe().setToUnfiltered();
        }
    }

    if (!isUnfiltered) {
        for (int i = 0; i < initialPositions.size(); i++) {
            auto pos = initialPositions[i];
            localScanState->nodeIDVector->state->getSelVectorUnsafe()[i] = pos;
            for (auto& outputVector : localScanState->outputVectors) {
                outputVector->state->getSelVectorUnsafe()[i] = pos;
            }
        }
    }

    return true;
}

} // namespace processor
} // namespace kuzu
