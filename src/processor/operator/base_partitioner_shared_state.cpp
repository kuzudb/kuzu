#include "processor/operator/base_partitioner_shared_state.h"

#include "main/client_context.h"
#include "storage/table/node_table.h"

namespace kuzu::processor {
void PartitionerSharedState::initialize(const common::logical_type_vec_t&,
    common::idx_t numPartitioners, const main::ClientContext* clientContext) {
    KU_ASSERT(numPartitioners >= 1 && numPartitioners <= DIRECTIONS);
    numNodes[0] = srcNodeTable->getNumTotalRows(clientContext->getTransaction());
    if (numPartitioners > 1) {
        numNodes[1] = dstNodeTable->getNumTotalRows(clientContext->getTransaction());
    }
    numPartitions[0] = getNumPartitionsFromRows(numNodes[0]);
    if (numPartitioners > 1) {
        numPartitions[1] = getNumPartitionsFromRows(numNodes[1]);
    }
}

common::partition_idx_t PartitionerSharedState::getNextPartition(common::idx_t partitioningIdx) {
    auto nextPartitionIdxToReturn = nextPartitionIdx++;
    if (nextPartitionIdxToReturn >= numPartitions[partitioningIdx]) {
        return common::INVALID_PARTITION_IDX;
    }
    return nextPartitionIdxToReturn;
}

common::partition_idx_t PartitionerSharedState::getNumPartitionsFromRows(common::offset_t numRows) {
    return (numRows + common::StorageConfig::NODE_GROUP_SIZE - 1) /
           common::StorageConfig::NODE_GROUP_SIZE;
}

void PartitionerSharedState::resetState(common::idx_t) {
    nextPartitionIdx = 0;
}
} // namespace kuzu::processor
