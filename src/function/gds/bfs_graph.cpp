#include "function/gds/bfs_graph.h"

using namespace kuzu::common;

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

void BFSGraph::addParent(uint16_t iter, nodeID_t boundNodeID, relID_t edgeID, nodeID_t nbrNodeID,
    bool fwdEdge, ObjectBlock<ParentList>* block) {
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setIter(iter);
    // Since by default the parentPtr of each node is nullptr, that's what we start with.
    ParentList* expected = nullptr;
    while (!currParentPtrs[nbrNodeID.offset].compare_exchange_strong(expected, parent))
        ;
    parent->setNextPtr(expected);
}

void BFSGraph::addSingleParent(uint16_t iter, nodeID_t boundNodeID, relID_t edgeID,
    nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block) {
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setIter(iter);
    ParentList* expected = nullptr;
    if (currParentPtrs[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
        parent->setNextPtr(expected);
    } else {
        // Other thread has added the parent. Do NOT add parent and revert reserved slot.
        block->revertLast();
    }
}

static double getCost(ParentList* parentList) {
    return parentList == nullptr ? std::numeric_limits<double>::max() : parentList->getCost();
}

bool BFSGraph::tryAddSingleParentWithWeight(nodeID_t boundNodeID, relID_t edgeID,
    nodeID_t nbrNodeID, bool fwdEdge, double weight, ObjectBlock<ParentList>* block) {
    ParentList* expected = getParentListHead(nbrNodeID.offset);
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setCost(getParentListHead(boundNodeID.offset)->getCost() + weight);
    while (parent->getCost() < getCost(expected)) {
        if (currParentPtrs[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
            // Since each node can have one parent, set next ptr to nullptr.
            parent->setNextPtr(nullptr);
            return true;
        }
    }
    // Other thread has added the parent. Do NOT add parent and revert reserved slot.
    block->revertLast();
    return false;
}

bool BFSGraph::tryAddParentWithWeight(nodeID_t boundNodeID, relID_t edgeID, nodeID_t nbrNodeID,
    bool fwdEdge, double weight, ObjectBlock<ParentList>* block) {
    ParentList* expected = getParentListHead(nbrNodeID.offset);
    auto parent = block->reserveNext();
    parent->setNbrInfo(boundNodeID, edgeID, fwdEdge);
    parent->setCost(getParentListHead(boundNodeID.offset)->getCost() + weight);
    while (true) {
        if (parent->getCost() < getCost(expected)) {
            // New parent has smaller cost, erase all existing parents and add new parent.
            if (currParentPtrs[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
                parent->setNextPtr(nullptr);
                return true;
            }
        } else if (parent->getCost() == getCost(expected)) {
            // New parent has the same cost, append new parent as after existing parents.
            if (currParentPtrs[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
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
