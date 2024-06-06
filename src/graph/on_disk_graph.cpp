#include "graph/on_disk_graph.h"

#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace graph {

NbrScanState::NbrScanState(MemoryManager* mm) {
    srcNodeIDVectorState = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVectorState = std::make_shared<DataChunkState>();
    srcNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->state = srcNodeIDVectorState;
    dstNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector->state = dstNodeIDVectorState;
    fwdReadState = std::make_unique<RelTableScanState>(columnIDs, direction);
    fwdReadState->nodeIDVector = srcNodeIDVector.get();
    fwdReadState->outputVectors.push_back(dstNodeIDVector.get());
}

OnDiskGraph::OnDiskGraph(ClientContext* context, common::table_id_t nodeTableID,
    common::table_id_t relTableID)
    : context{context} {
    auto storage = context->getStorageManager();
    nodeTable = storage->getTable(nodeTableID)->ptrCast<NodeTable>();
    relTable = storage->getTable(relTableID)->ptrCast<RelTable>();
    nbrScanState = std::make_unique<NbrScanState>(context->getMemoryManager());
}

offset_t OnDiskGraph::getNumNodes() {
    return nodeTable->getNumTuples(context->getTx());
}

offset_t OnDiskGraph::getNumEdges() {
    return relTable->getNumTuples(context->getTx());
}

std::vector<nodeID_t> OnDiskGraph::getNbrs(offset_t offset) {
    nbrScanState->srcNodeIDVector->setValue<nodeID_t>(0, {offset, nodeTable->getTableID()});
    auto tx = context->getTx();
    auto readState = nbrScanState->fwdReadState.get();
    auto dstState = nbrScanState->dstNodeIDVectorState.get();
    auto dstVector = nbrScanState->dstNodeIDVector.get();
    std::vector<nodeID_t> nbrs;
    relTable->initializeScanState(tx, *readState);
    while (nbrScanState->fwdReadState->hasMoreToRead(tx)) {
        relTable->scan(tx, *readState);
        KU_ASSERT(dstState->getSelVector().isUnfiltered());
        for (auto i = 0u; i < dstState->getSelVector().getSelSize(); ++i) {
            auto nodeID = dstVector->getValue<nodeID_t>(i);
            nbrs.push_back(nodeID);
        }
    }
    return nbrs;
}

} // namespace graph
} // namespace kuzu
