#include "storage/store/csr_node_group_collection.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

CSRNodeGroupCollection::CSRNodeGroupCollection(const std::vector<LogicalType>& types,
    bool enableCompression, offset_t startNodeOffset, BMFileHandle* dataFH, Deserializer* deSer)
    : enableCompression{enableCompression}, startNodeOffset{startNodeOffset}, numRows{0},
      types{types}, dataFH{dataFH} {
    if (deSer) {
        KU_ASSERT(dataFH);
        nodeGroups.loadGroups(*deSer);
    }
    const auto lock = nodeGroups.lock();
    for (auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        numRows += nodeGroup->getNumRows();
    }
}

void CSRNodeGroupCollection::append(Transaction* transaction, ValueVector* srcVector,
    std::vector<ValueVector*> dataVectors) {
    // TODO: Implement this
}

uint64_t CSRNodeGroupCollection::getEstimatedMemoryUsage() {
    // TODO: Implement this
}

void CSRNodeGroupCollection::checkpoint(const NodeGroupCheckpointState& state) {
    // TODO: Implement this
}

} // namespace storage
} // namespace kuzu
