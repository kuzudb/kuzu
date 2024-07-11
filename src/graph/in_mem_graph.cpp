#include "graph/in_mem_graph.h"

#include "main/client_context.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace graph {

InMemGraph::InMemGraph(ClientContext* context, common::table_id_t nodeTableID,
    common::table_id_t relTableID, std::shared_ptr<CSRIndexSharedState> csrIndexSharedState)
    : context{context} {
    auto storage = context->getStorageManager();
    nodeTable = storage->getTable(nodeTableID)->ptrCast<NodeTable>();
    relTable = storage->getTable(relTableID)->ptrCast<RelTable>();
    isInMemory = true;
    csrSharedState = csrIndexSharedState;
}

offset_t InMemGraph::getNumNodes() {
    return nodeTable->getNumTuples(context->getTx());
}

offset_t InMemGraph::getNumEdges() {
    return relTable->getNumTuples(context->getTx());
}

std::vector<nodeID_t> InMemGraph::getNbrs(offset_t offset, NbrScanState* nbrScanState) {
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

void InMemGraph::initializeStateFwdNbrs(common::offset_t offset, NbrScanState* nbrScanState) {
    nbrScanState->srcNodeIDVector->setValue<nodeID_t>(0, {offset, nodeTable->getTableID()});
    relTable->initializeScanState(context->getTx(), *nbrScanState->fwdReadState.get());
}

bool InMemGraph::hasMoreFwdNbrs(NbrScanState* nbrScanState) {
    return nbrScanState->fwdReadState->hasMoreToRead(context->getTx());
}

void InMemGraph::getFwdNbrs(NbrScanState* nbrScanState) {
    auto tx = context->getTx();
    auto readState = nbrScanState->fwdReadState.get();
    relTable->scan(tx, *readState);
}

} // namespace graph
} // namespace kuzu
