#include "storage/store/csr_node_group.h"

#include "storage/store/rel_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void CSRNodeGroup::initializeScanState(Transaction* transaction, TableScanState& state) {
    auto& relScanState = state.cast<RelTableScanState>();
    KU_ASSERT(nodeGroupIdx == StorageUtils::getNodeGroupIdx(relScanState.boundNodeOffset));
    auto& nodeGroupScanState = relScanState.nodeGroupScanState;
    if (relScanState.nodeGroupIdx != nodeGroupIdx) {
        // Initialize the scan state for checkpointed data in the node group.
        nodeGroupScanState.checkpointedHeader = std::make_unique<ChunkedCSRHeader>();
        relScanState.nodeGroupIdx = nodeGroupIdx;
        // Scan the csr header chunks from disk.
        ChunkState offsetState, lengthState;
        checkpointedGroup->getCSRHeader().offset->initializeScanState(offsetState);
        checkpointedGroup->getCSRHeader().length->initializeScanState(lengthState);
        offsetState.column = relScanState.csrHeaderColumns.offset.get();
        lengthState.column = relScanState.csrHeaderColumns.length.get();
        nodeGroupScanState.checkpointedHeader->offset->scanCommitted<ResidencyState::ON_DISK>(
            transaction, offsetState, *nodeGroupScanState.checkpointedHeader->offset);
        nodeGroupScanState.checkpointedHeader->length->scanCommitted<ResidencyState::ON_DISK>(
            transaction, lengthState, *nodeGroupScanState.checkpointedHeader->length);
        // Initialize the scan state for the persisted data in the node group.
        relScanState.zoneMapResult = ZoneMapCheckResult::ALWAYS_SCAN;
        for (auto i = 0u; i < relScanState.columnIDs.size(); i++) {
            if (relScanState.columnIDs[i] == INVALID_COLUMN_ID) {
                continue;
            }
            auto& chunk = checkpointedGroup->getColumnChunk(relScanState.columnIDs[i]);
            chunk.initializeScanState(nodeGroupScanState.chunkStates[i]);
            // TODO: Not a good way to initialize column for chunkState here.
            nodeGroupScanState.chunkStates[i].column = state.columns[i];
        }
    }
    // Switch to a new bound node (i.e., new csr list) in the node group.
    nodeGroupScanState.nextRowToScan = 0;
    nodeGroupScanState.checkpointedCSRList = csr_list_t();
}

bool CSRNodeGroup::scan(Transaction* transaction, TableScanState& state) {}

void CSRNodeGroup::serialize(Serializer& serializer) const {
    serializer.write<node_group_idx_t>(nodeGroupIdx);
    serializer.write<bool>(enableCompression);
    serializer.write<bool>(checkpointedGroup != nullptr);
    if (checkpointedGroup) {
        checkpointedGroup->serialize(serializer);
    }
}

std::unique_ptr<CSRNodeGroup> CSRNodeGroup::deserialize(Deserializer& deSer) {
    node_group_idx_t nodeGroupIdx;
    bool enableCompression;
    bool hasCheckpointedData;
    deSer.deserializeValue<node_group_idx_t>(nodeGroupIdx);
    deSer.deserializeValue<bool>(enableCompression);
    deSer.deserializeValue<bool>(hasCheckpointedData);
    if (!hasCheckpointedData) {
        return nullptr;
    }
    auto checkpointedGroup = ChunkedCSRNodeGroup::deserialize(deSer);
    return std::make_unique<CSRNodeGroup>(nodeGroupIdx, enableCompression,
        std::move(checkpointedGroup));
}

} // namespace storage
} // namespace kuzu
