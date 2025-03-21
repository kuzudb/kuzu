#include "function/gds/bfs_graph.h"
#include "processor/execution_context.h"
#include "function/gds/gds_utils.h"

using namespace kuzu::common;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

static constexpr uint64_t BFS_GRAPH_BLOCK_SIZE = (std::uint64_t)1 << 19;

ObjectBlock<ParentList>* BFSGraph::addNewBlock() {
    std::unique_lock lck{mtx};
    auto memBlock = mm->allocateBuffer(false /* init to 0 */, BFS_GRAPH_BLOCK_SIZE);
    blocks.push_back(
        std::make_unique<ObjectBlock<ParentList>>(std::move(memBlock), BFS_GRAPH_BLOCK_SIZE));
    return blocks[blocks.size() - 1].get();
}

void BFSGraph::initDenseObjects(ExecutionContext* context, Graph* graph) {
    for (auto& [tableID, maxOffset] : maxOffsetMap) {
        denseObjects.allocate(tableID, maxOffset, context->clientContext->getMemoryManager());
    }
    auto vc = std::make_unique<BFSGraphInitVertexCompute>(*this);
    GDSUtils::runVertexCompute(context, graph, *vc);
}

void BFSGraph::pinTableID(table_id_t tableID) {
    switch (state) {
    case GDSDensityState::SPARSE: {
        curDenseData = denseObjects.getData(tableID);
    } break;
    case GDSDensityState::DENSE: {
        curSparseData = sparseObjects.getMap(tableID);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void BFSGraph::addParentSparse(uint16_t iter, nodeID_t boundNodeID, relID_t edgeID,
    nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block) {
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setIter(iter);
    if (curSparseData->contains(nbrNodeID.offset)) {
        parent->setNextPtr(curSparseData->at(nbrNodeID.offset));
    } else {
        parent->setNextPtr(nullptr);
        curSparseData->insert({nbrNodeID.offset, nullptr});
    }
    curSparseData->at(nbrNodeID.offset).store(parent, std::memory_order_relaxed);
}

void BFSGraph::addParentDense(uint16_t iter, nodeID_t boundNodeID, relID_t edgeID, nodeID_t nbrNodeID,
    bool fwdEdge, ObjectBlock<ParentList>* block) {
    KU_ASSERT(state == GDSDensityState::DENSE && curDenseData);
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setIter(iter);
    // Since by default the parentPtr of each node is nullptr, that's what we start with.
    ParentList* expected = nullptr;
    while (!curDenseData[nbrNodeID.offset].compare_exchange_strong(expected, parent))
        ;
    parent->setNextPtr(expected);
}

void BFSGraph::addSingleParentDense(uint16_t iter, nodeID_t boundNodeID, relID_t edgeID,
    nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block) {
    KU_ASSERT(state == GDSDensityState::DENSE && curDenseData);
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setIter(iter);
    ParentList* expected = nullptr;
    if (curDenseData[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
        parent->setNextPtr(expected);
    } else {
        // Other thread has added the parent. Do NOT add parent and revert reserved slot.
        block->revertLast();
    }
}

static double getCost(ParentList* parentList) {
    return parentList == nullptr ? std::numeric_limits<double>::max() : parentList->getCost();
}

bool BFSGraph::tryAddSingleParentWithWeightDense(nodeID_t boundNodeID, relID_t edgeID,
    nodeID_t nbrNodeID, bool fwdEdge, double weight, ObjectBlock<ParentList>* block) {
    ParentList* expected = getParentListHead(nbrNodeID.offset);
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setCost(getParentListHead(boundNodeID.offset)->getCost() + weight);
    while (parent->getCost() < getCost(expected)) {
        if (curDenseData[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
            // Since each node can have one parent, set next ptr to nullptr.
            parent->setNextPtr(nullptr);
            return true;
        }
    }
    // Other thread has added the parent. Do NOT add parent and revert reserved slot.
    block->revertLast();
    return false;
}

bool BFSGraph::tryAddParentWithWeightDense(nodeID_t boundNodeID, relID_t edgeID, nodeID_t nbrNodeID,
    bool fwdEdge, double weight, ObjectBlock<ParentList>* block) {
    ParentList* expected = getParentListHead(nbrNodeID.offset);
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setCost(getParentListHead(boundNodeID.offset)->getCost() + weight);
    while (true) {
        if (parent->getCost() < getCost(expected)) {
            // New parent has smaller cost, erase all existing parents and add new parent.
            if (curDenseData[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
                parent->setNextPtr(nullptr);
                return true;
            }
        } else if (parent->getCost() == getCost(expected)) {
            // New parent has the same cost, append new parent as after existing parents.
            if (curDenseData[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
                parent->setNextPtr(expected);
                return true;
            }
        } else {
            block->revertLast();
            return false;
        }
    }
}

} // namespace function
} // namespace kuzu
