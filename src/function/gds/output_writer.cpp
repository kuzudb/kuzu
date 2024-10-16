#include "function/gds/output_writer.h"

#include "function/gds/gds_frontier.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

SPOutputs::SPOutputs(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
    nodeID_t sourceNodeID, storage::MemoryManager* mm)
    : RJOutputs(sourceNodeID) {
    pathLengths = std::make_shared<PathLengths>(nodeTableIDAndNumNodes, mm);
}

void PathsOutputs::beginWritingOutputsForDstNodesInTable(common::table_id_t tableID) {
    pathLengths->fixCurFrontierNodeTable(tableID);
    bfsGraph.pinNodeTable(tableID);
}

static std::unique_ptr<ValueVector> createVector(const LogicalType& type,
    storage::MemoryManager* mm, std::vector<ValueVector*>& vectors) {
    auto vector = std::make_unique<ValueVector>(type.copy(), mm);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(vector.get());
    return vector;
}

RJOutputWriter::RJOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs)
    : context{context}, rjOutputs{rjOutputs} {
    auto mm = context->getMemoryManager();
    srcNodeIDVector = createVector(LogicalType::INTERNAL_ID(), mm, vectors);
    dstNodeIDVector = createVector(LogicalType::INTERNAL_ID(), mm, vectors);
    srcNodeIDVector->setValue<nodeID_t>(0, rjOutputs->sourceNodeID);
}

PathsOutputWriter::PathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
    uint16_t lowerBound, uint16_t upperBound, bool extendFromSource, bool writeEdgeDirection)
    : RJOutputWriter(context, rjOutputs), lowerBound{lowerBound}, upperBound{upperBound},
      extendFromSource{extendFromSource}, writeEdgeDirection{writeEdgeDirection} {
    auto mm = context->getMemoryManager();
    if (writeEdgeDirection) {
        directionVector = createVector(LogicalType::LIST(LogicalType::BOOL()), mm, vectors);
    } else {
        directionVector = nullptr;
    }
    lengthVector = createVector(LogicalType::INT64(), mm, vectors);
    pathNodeIDsVector = createVector(LogicalType::LIST(LogicalType::INTERNAL_ID()), mm, vectors);
    pathEdgeIDsVector = createVector(LogicalType::LIST(LogicalType::INTERNAL_ID()), mm, vectors);
}

static void addListEntry(ValueVector* vector, uint64_t length) {
    vector->resetAuxiliaryBuffer();
    auto entry = ListVector::addList(vector, length);
    KU_ASSERT(entry.offset == 0);
    vector->setValue(0, entry);
}

void PathsOutputWriter::write(processor::FactorizedTable& fTable, nodeID_t dstNodeID) const {
    auto output = rjOutputs->ptrCast<PathsOutputs>();
    auto& bfsGraph = output->bfsGraph;
    auto sourceNodeID = output->sourceNodeID;
    dstNodeIDVector->setValue<common::nodeID_t>(0, dstNodeID);
    auto firstParent =
        bfsGraph.getCurFixedParentPtrs()[dstNodeID.offset].load(std::memory_order_relaxed);
    if (firstParent == nullptr) {
        if (sourceNodeID == dstNodeID) {
            // We still output a path from src to src if required path length is 0.
            // This case should only run for variable length joins.
            lengthVector->setValue<int64_t>(0, 0);
            beginWritingNewPath(0);
            fTable.append(vectors);
        }
        return;
    }
    lengthVector->setValue<int64_t>(0, firstParent->getIter());
    std::vector<ParentList*> curPath;
    curPath.push_back(firstParent);
    auto backtracking = false;
    while (!curPath.empty()) {
        auto top = curPath[curPath.size() - 1];
        auto topNodeID = top->getNodeID();
        if (topNodeID == sourceNodeID) {
            beginWritingNewPath(curPath.size());
            if (extendFromSource) {
                // Write path in reverse direction because we append ParentList from dst to src.
                for (auto i = 1u; i < curPath.size(); i++) {
                    auto p = curPath[curPath.size() - 1 - i];
                    addNode(p->getNodeID(), i - 1);
                    addEdge(p->getEdgeID(), p->isFwdEdge(), i);
                }
                auto lastPathElement = curPath[curPath.size() - 1];
                addEdge(lastPathElement->getEdgeID(), lastPathElement->isFwdEdge(), 0);
            } else {
                // Write path in original direction because computation started from dst node.
                // We want to present result in src->dst order.
                for (auto i = 0u; i < curPath.size() - 1; i++) {
                    auto p = curPath[i];
                    addNode(p->getNodeID(), i);
                    addEdge(p->getEdgeID(), p->isFwdEdge(), i);
                }
                auto lastPathElement = curPath[curPath.size() - 1];
                addEdge(lastPathElement->getEdgeID(), lastPathElement->isFwdEdge(),
                    curPath.size() - 1);
            }

            fTable.append(vectors);
            backtracking = true;
        }
        if (backtracking) {
            // This code checks if we should switch from backtracking to forward-tracking, i.e.,
            // moving forward in the DFS logic to find paths. We switch from backtracking if:
            // (i) the current top element in the stack has a nextPtr, i.e., the top node has
            // more parent edges in the BFS graph AND:
            // (ii.1) if this is the first element in the stack (curPath.size() == 1), i.e., we
            // are enumerating the parents of the destination, then we should switch to
            // forward-tracking if the next parent has visited the destination at a length
            // that's greater than or equal to the lower bound of the recursive join. Otherwise,
            // we'll enumerate paths that are smaller than the lower; OR
            // (ii.2) if this is not the first element in the stack, i.e., then we should switch
            // to forward tracking only if the next parent of the top node in the stack has the
            // same iter value as the current parent. That's because the levels/iter need to
            // decrease by 1 each time we add a new node in the stack.
            if (top->getNextPtr() != nullptr &&
                ((curPath.size() == 1 && top->getNextPtr()->getIter() >= lowerBound) ||
                    top->getNextPtr()->getIter() == top->getIter())) {
                curPath[curPath.size() - 1] = top->getNextPtr();
                backtracking = false;
                if (curPath.size() == 1) {
                    lengthVector->setValue<int64_t>(0, curPath[0]->getIter());
                }
            } else {
                curPath.pop_back();
            }
        } else {
            auto parent = bfsGraph.getInitialParentAndNextPtr(topNodeID);
            while (parent->getIter() != top->getIter() - 1) {
                parent = parent->getNextPtr();
            }
            curPath.push_back(parent);
            backtracking = false;
        }
    }
}

void PathsOutputWriter::beginWritingNewPath(uint64_t length) const {
    addListEntry(pathNodeIDsVector.get(), length > 1 ? length - 1 : 0);
    addListEntry(pathEdgeIDsVector.get(), length);
    if (writeEdgeDirection) {
        addListEntry(directionVector.get(), length);
    }
}

void PathsOutputWriter::addEdge(relID_t edgeID, bool fwdEdge, sel_t pos) const {
    ListVector::getDataVector(pathEdgeIDsVector.get())->setValue(pos, edgeID);
    if (writeEdgeDirection) {
        ListVector::getDataVector(directionVector.get())->setValue(pos, fwdEdge);
    }
}

void PathsOutputWriter::addNode(common::nodeID_t nodeID, common::sel_t pos) const {
    ListVector::getDataVector(pathNodeIDsVector.get())->setValue(pos, nodeID);
}

void DestinationsOutputWriter::write(processor::FactorizedTable& fTable, nodeID_t dstNodeID) const {
    auto length =
        rjOutputs->ptrCast<SPOutputs>()->pathLengths->getMaskValueFromCurFrontierFixedMask(
            dstNodeID.offset);
    dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
    writeMoreAndAppend(fTable, dstNodeID, length);
}

bool DestinationsOutputWriter::skipWriting(common::nodeID_t dstNodeID) const {
    return dstNodeID == rjOutputs->ptrCast<SPOutputs>()->sourceNodeID ||
           PathLengths::UNVISITED ==
               rjOutputs->ptrCast<SPOutputs>()->pathLengths->getMaskValueFromCurFrontierFixedMask(
                   dstNodeID.offset);
}

void DestinationsOutputWriter::writeMoreAndAppend(processor::FactorizedTable& fTable, nodeID_t,
    uint16_t) const {
    fTable.append(vectors);
}

} // namespace function
} // namespace kuzu
