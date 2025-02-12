#include "function/gds/bfs_graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static constexpr uint64_t BFS_GRAPH_BLOCK_SIZE = (std::uint64_t)1 << 19;

ObjectBlock<ParentList>* BFSGraph::addNewBlock() {
    return addNewBlock(BFS_GRAPH_BLOCK_SIZE);
}

ObjectBlock<ParentList>* BFSGraph::addNewBlock(uint64_t size) {
    std::unique_lock lck{mtx};
    auto memBlock = mm->allocateBuffer(false /* init to 0 */, size);
    blocks.push_back(
        std::make_unique<ObjectBlock<ParentList>>(std::move(memBlock), size));
    return blocks[blocks.size() - 1].get();
}

void BFSGraph::addParent(uint16_t iter, nodeID_t boundNodeID, relID_t edgeID, nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block) {
    auto parent = block->reserveNext();
    parent->store(iter, boundNodeID, edgeID, fwdEdge);
    // Since by default the parentPtr of each node is nullptr, that's what we start with.
    ParentList* expected = nullptr;
    while (!currParentPtrs[nbrNodeID.offset].compare_exchange_strong(expected, parent));
    parent->setNextPtr(expected);
}

void BFSGraph::addSingleParent(uint16_t iter, nodeID_t boundNodeID, relID_t edgeID, nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<kuzu::function::ParentList>* block) {
    auto parent = block->reserveNext();
    parent->store(iter, boundNodeID, edgeID, fwdEdge);
    ParentList* expected = nullptr;
    if (currParentPtrs[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
        parent->setNextPtr(expected);
    } else {
        // Do NOT add parent and revert reserved slot.
        block->revertLast();
    }
}

static double getCost(ParentList* parentList) {
    return parentList == nullptr? std::numeric_limits<double>::max() : parentList->cost;
}

bool BFSGraph::tryAddSingleParentWithWeight(nodeID_t boundNodeID, relID_t edgeID, nodeID_t nbrNodeID, bool fwdEdge,
    double weight, ObjectBlock<ParentList>* block) {
    ParentList* expected = getParentListHead(nbrNodeID.offset);
    auto parent = block->reserveNext();
    parent->store(UINT16_MAX /* iter */, boundNodeID, edgeID, fwdEdge);
    parent->cost = getParentListHead(boundNodeID.offset)->cost + weight;;
    while (parent->cost < getCost(expected)) {
        if (currParentPtrs[nbrNodeID.offset].compare_exchange_strong(expected, parent)) {
            parent->setNextPtr(nullptr);
            return true;
        }
    }
    block->revertLast();
    return false;
}

}
}
