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

RJOutputWriter::RJOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs)
    : context{context}, rjOutputs{rjOutputs} {
    auto mm = context->getMemoryManager();
    srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    srcNodeIDVector->setValue<nodeID_t>(0, rjOutputs->sourceNodeID);
    dstNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(srcNodeIDVector.get());
    vectors.push_back(dstNodeIDVector.get());
}

PathsOutputWriter::PathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
    uint16_t lowerBound, uint16_t upperBound)
    : RJOutputWriter(context, rjOutputs), lowerBound{lowerBound}, upperBound{upperBound} {
    lengthVector = std::make_unique<ValueVector>(LogicalType::INT64(), context->getMemoryManager());
    lengthVector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(lengthVector.get());
    pathNodeIDsVector = std::make_unique<ValueVector>(LogicalType::LIST(LogicalType::INTERNAL_ID()),
        context->getMemoryManager());
    pathNodeIDsVector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(pathNodeIDsVector.get());
    pathEdgeIDsVector = std::make_unique<ValueVector>(LogicalType::LIST(LogicalType::INTERNAL_ID()),
        context->getMemoryManager());
    pathEdgeIDsVector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(pathEdgeIDsVector.get());
}

static void addListEntry(ValueVector* vector, uint64_t length) {
    vector->resetAuxiliaryBuffer();
    auto entry = ListVector::addList(vector, length);
    KU_ASSERT(entry.offset == 0);
    vector->setValue(0, entry);
}

/**
 * Helper class to write paths to a ValueVector. The ValueVector should be a ListVector. The path
 * is written in reverse order, so the calls to addNewNodeID should be done in the reverse order of
 * the nodes that should be placed into the ListVector.
 */
class PathVectorWriter {
public:
    PathVectorWriter(common::ValueVector* nodeIDsVector, common::ValueVector* edgeIDsVector)
        : nodeIDsVector{nodeIDsVector}, edgeIDsVector{edgeIDsVector} {}
    void beginWritingNewPath(uint64_t length) const {
        addListEntry(nodeIDsVector, length > 1 ? length - 1 : 0);
        if (edgeIDsVector != nullptr) {
            addListEntry(edgeIDsVector, length);
        }
    }
    void addEdge(common::relID_t edgeID, common::sel_t pos) const {
        ListVector::getDataVector(edgeIDsVector)->setValue(pos, edgeID);
    }
    void addNodeEdge(common::nodeID_t nodeID, common::relID_t edgeID, common::sel_t pos) const {
        KU_ASSERT(pos > 0);
        ListVector::getDataVector(nodeIDsVector)->setValue(pos - 1, nodeID);
        ListVector::getDataVector(edgeIDsVector)->setValue(pos, edgeID);
    }

public:
    common::ValueVector* nodeIDsVector;
    common::ValueVector* edgeIDsVector;
};

void PathsOutputWriter::write(processor::FactorizedTable& fTable, nodeID_t dstNodeID) const {
    auto output = rjOutputs->ptrCast<PathsOutputs>();
    auto& bfsGraph = output->bfsGraph;
    auto sourceNodeID = output->sourceNodeID;
    PathVectorWriter writer(pathNodeIDsVector.get(), pathEdgeIDsVector.get());
    dstNodeIDVector->setValue<common::nodeID_t>(0, dstNodeID);
    auto firstParent =
        bfsGraph.getCurFixedParentPtrs()[dstNodeID.offset].load(std::memory_order_relaxed);

    if (firstParent == nullptr) {
        // This case should only run for variable length joins.
        lengthVector->setValue<int64_t>(0, 0);
        writer.beginWritingNewPath(0);
        fTable.append(vectors);
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
            writer.beginWritingNewPath(curPath.size());
            for (auto i = 1u; i < curPath.size(); i++) {
                auto p = curPath[curPath.size() - 1 - i];
                writer.addNodeEdge(p->getNodeID(), p->getEdgeID(), i);
            }
            writer.addEdge(curPath[curPath.size() - 1]->getEdgeID(), 0);

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
