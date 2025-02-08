#include "graph/graph.h"

#include "common/system_config.h"

namespace kuzu::graph {
NbrScanState::Chunk::Chunk(std::span<const common::nodeID_t> nbrNodes,
    std::span<const common::relID_t> edges, common::SelectionVector& selVector,
    const common::ValueVector* propertyVector)
    : nbrNodes{nbrNodes}, edges{edges}, selVector{selVector}, propertyVector{propertyVector} {
    KU_ASSERT(nbrNodes.size() == common::DEFAULT_VECTOR_CAPACITY);
    KU_ASSERT(edges.size() == common::DEFAULT_VECTOR_CAPACITY);
}

VertexScanState::Chunk::Chunk(std::span<const common::nodeID_t> nodeIDs,
    std::span<const std::shared_ptr<common::ValueVector>> propertyVectors)
    : nodeIDs{nodeIDs}, propertyVectors{propertyVectors} {
    KU_ASSERT(nodeIDs.size() <= common::DEFAULT_VECTOR_CAPACITY);
}
} // namespace kuzu::graph
