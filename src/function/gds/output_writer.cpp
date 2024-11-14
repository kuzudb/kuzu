#include "function/gds/output_writer.h"

#include "common/types/internal_id_util.h"
#include "function/gds/gds_frontier.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

SPOutputs::SPOutputs(common::table_id_map_t<common::offset_t> numNodesMap, nodeID_t sourceNodeID,
    storage::MemoryManager* mm)
    : RJOutputs(sourceNodeID) {
    pathLengths = std::make_shared<PathLengths>(numNodesMap, mm);
}

void PathsOutputs::beginWritingOutputsForDstNodesInTable(common::table_id_t tableID) {
    pathLengths->pinCurFrontierTableID(tableID);
    bfsGraph.pinNodeTable(tableID);
}

RJOutputWriter::RJOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
    processor::NodeOffsetMaskMap* outputNodeMask)
    : context{context}, rjOutputs{rjOutputs}, outputNodeMask{outputNodeMask} {
    auto mm = context->getMemoryManager();
    srcNodeIDVector = createVector(LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector = createVector(LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->setValue<nodeID_t>(0, rjOutputs->sourceNodeID);
}

std::unique_ptr<common::ValueVector> RJOutputWriter::createVector(const LogicalType& type,
    storage::MemoryManager* mm) {
    auto vector = std::make_unique<ValueVector>(type.copy(), mm);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(vector.get());
    return vector;
}

void RJOutputWriter::beginWritingForDstNodesInTable(common::table_id_t tableID) {
    rjOutputs->beginWritingOutputsForDstNodesInTable(tableID);
    if (outputNodeMask != nullptr) {
        outputNodeMask->pin(tableID);
    }
}

bool RJOutputWriter::skip(common::nodeID_t dstNodeID) const {
    if (outputNodeMask != nullptr && outputNodeMask->hasPinnedMask()) {
        auto mask = outputNodeMask->getPinnedMask();
        if (mask->isEnabled() && !mask->isMasked(dstNodeID.offset)) {
            return true;
        }
    }
    return skipInternal(dstNodeID);
}

PathsOutputWriter::PathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
    processor::NodeOffsetMaskMap* outputNodeMask, PathsOutputWriterInfo info)
    : RJOutputWriter{context, rjOutputs, outputNodeMask}, info{info} {
    auto mm = context->getMemoryManager();
    if (info.writeEdgeDirection) {
        directionVector = createVector(LogicalType::LIST(LogicalType::BOOL()), mm);
    }
    lengthVector = createVector(LogicalType::UINT16(), mm);
    if (info.writePath) {
        pathNodeIDsVector = createVector(LogicalType::LIST(LogicalType::INTERNAL_ID()), mm);
        pathEdgeIDsVector = createVector(LogicalType::LIST(LogicalType::INTERNAL_ID()), mm);
    }
}

static void addListEntry(ValueVector* vector, uint64_t length) {
    vector->resetAuxiliaryBuffer();
    auto entry = ListVector::addList(vector, length);
    KU_ASSERT(entry.offset == 0);
    vector->setValue(0, entry);
}

static void setLength(ValueVector* vector, uint16_t length) {
    KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UINT16);
    vector->setValue<uint16_t>(0, length);
}

void PathsOutputWriter::write(processor::FactorizedTable& fTable, nodeID_t dstNodeID,
    GDSOutputCounter* counter) {
    auto output = rjOutputs->ptrCast<PathsOutputs>();
    auto& bfsGraph = output->bfsGraph;
    auto sourceNodeID = output->sourceNodeID;
    dstNodeIDVector->setValue<common::nodeID_t>(0, dstNodeID);
    auto firstParent = bfsGraph.getParentListHead(dstNodeID.offset);
    if (firstParent == nullptr) {
        if (sourceNodeID == dstNodeID) {
            // We still output a path from src to src if required path length is 0.
            // This case should only run for variable length joins.
            setLength(lengthVector.get(), 0);
            if (info.writePath) {
                beginWritePath(0);
            }
            fTable.append(vectors);
            // No need to check against limit number since this is the first output.
            if (counter != nullptr) {
                counter->increase(1);
            }
        }
        return;
    }
    setLength(lengthVector.get(), firstParent->getIter());
    std::vector<ParentList*> curPath;
    curPath.push_back(firstParent);
    auto backtracking = false;
    while (!curPath.empty()) {
        auto top = curPath[curPath.size() - 1];
        auto topNodeID = top->getNodeID();
        if (top->getIter() == 1) {
            // check path
            if (checkPathNodeMask(curPath) && checkSemantic(curPath)) {
                writePath(curPath);
                fTable.append(vectors);
                if (counter != nullptr) {
                    counter->increase(1);
                    if (counter->exceedLimit()) {
                        return;
                    }
                }
            }
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
                ((curPath.size() == 1 && top->getNextPtr()->getIter() >= info.lowerBound) ||
                    top->getNextPtr()->getIter() == top->getIter())) {
                curPath[curPath.size() - 1] = top->getNextPtr();
                backtracking = false;
                if (curPath.size() == 1) {
                    setLength(lengthVector.get(), curPath[0]->getIter());
                }
            } else {
                curPath.pop_back();
            }
        } else {
            auto parent = bfsGraph.getParentListHead(topNodeID);
            while (parent->getIter() != top->getIter() - 1) {
                parent = parent->getNextPtr();
            }
            curPath.push_back(parent);
            backtracking = false;
        }
    }
    return;
}

bool PathsOutputWriter::checkPathNodeMask(const std::vector<ParentList*>& path) const {
    if (!info.hasNodeMask()) {
        return true;
    }
    // Path node predicate should only be applied to intermediate node. So we exclude dst node.
    for (auto i = 0u; i < path.size() - 1; ++i) {
        if (!info.pathNodeMask->valid(path[i]->getNodeID())) {
            return false;
        }
    }
    return true;
}

// TODO(Xiyang): apply semantic check during DFS backtracking.
bool PathsOutputWriter::checkSemantic(const std::vector<ParentList*>& path) const {
    switch (info.semantic) {
    case PathSemantic::WALK:
        return true;
    case PathSemantic::TRAIL:
        return isTrail(path);
    case PathSemantic::ACYCLIC:
        return isAcyclic(path);
    default:
        KU_UNREACHABLE;
    }
}

bool PathsOutputWriter::isTrail(const std::vector<ParentList*>& path) const {
    rel_id_set_t set;
    for (auto i = 0u; i < path.size(); ++i) {
        auto edgeID = path[i]->getEdgeID();
        if (set.contains(edgeID)) {
            return false;
        }
        set.insert(edgeID);
    }
    return true;
}

bool PathsOutputWriter::isAcyclic(const std::vector<ParentList*>& path) const {
    node_id_set_t set;
    for (auto i = 0u; i < path.size() - 1; ++i) {
        auto nodeID = path[i]->getNodeID();
        if (set.contains(nodeID)) {
            return false;
        }
        set.insert(nodeID);
    }
    return true;
}

void PathsOutputWriter::beginWritePath(common::idx_t length) const {
    KU_ASSERT(info.writePath);
    addListEntry(pathNodeIDsVector.get(), length > 1 ? length - 1 : 0);
    addListEntry(pathEdgeIDsVector.get(), length);
    if (info.writeEdgeDirection) {
        addListEntry(directionVector.get(), length);
    }
}

void PathsOutputWriter::writePath(const std::vector<ParentList*>& path) const {
    if (!info.writePath) {
        return;
    }
    beginWritePath(path.size());
    if (!info.flipPath) {
        // By default, write path in reverse direction because we append ParentList from dst to src.
        writePathBwd(path);
    } else {
        // Write path in original direction because computation started from dst node.
        // We want to present result in src->dst order.
        writePathFwd(path);
    }
}

void PathsOutputWriter::writePathFwd(const std::vector<ParentList*>& path) const {
    auto length = path.size();
    for (auto i = 0u; i < length - 1; i++) {
        auto p = path[i];
        addNode(p->getNodeID(), i);
        addEdge(p->getEdgeID(), p->isFwdEdge(), i);
    }
    auto lastPathElement = path[length - 1];
    addEdge(lastPathElement->getEdgeID(), lastPathElement->isFwdEdge(), length - 1);
}

void PathsOutputWriter::writePathBwd(const std::vector<ParentList*>& path) const {
    auto length = path.size();
    for (auto i = 1u; i < length; i++) {
        auto p = path[length - 1 - i];
        addNode(p->getNodeID(), i - 1);
        addEdge(p->getEdgeID(), p->isFwdEdge(), i);
    }
    auto lastPathElement = path[length - 1];
    addEdge(lastPathElement->getEdgeID(), lastPathElement->isFwdEdge(), 0);
}

void PathsOutputWriter::addEdge(relID_t edgeID, bool fwdEdge, sel_t pos) const {
    ListVector::getDataVector(pathEdgeIDsVector.get())->setValue(pos, edgeID);
    if (info.writeEdgeDirection) {
        ListVector::getDataVector(directionVector.get())->setValue(pos, fwdEdge);
    }
}

void PathsOutputWriter::addNode(common::nodeID_t nodeID, common::sel_t pos) const {
    ListVector::getDataVector(pathNodeIDsVector.get())->setValue(pos, nodeID);
}

DestinationsOutputWriter::DestinationsOutputWriter(main::ClientContext* context,
    RJOutputs* rjOutputs, processor::NodeOffsetMaskMap* outputNodeMask)
    : RJOutputWriter{context, rjOutputs, outputNodeMask} {
    lengthVector = createVector(LogicalType::UINT16(), context->getMemoryManager());
}

void DestinationsOutputWriter::write(processor::FactorizedTable& fTable, nodeID_t dstNodeID,
    GDSOutputCounter* counter) {
    auto length =
        rjOutputs->ptrCast<SPOutputs>()->pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset);
    dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
    setLength(lengthVector.get(), length);
    fTable.append(vectors);
    if (counter != nullptr) {
        counter->increase(1);
    }
    return;
}

bool DestinationsOutputWriter::skipInternal(common::nodeID_t dstNodeID) const {
    auto outputs = rjOutputs->ptrCast<SPOutputs>();
    return dstNodeID == outputs->sourceNodeID || outputs->pathLengths->getMaskValueFromCurFrontier(
                                                     dstNodeID.offset) == PathLengths::UNVISITED;
}

} // namespace function
} // namespace kuzu
