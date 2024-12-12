#include "function/gds/output_writer.h"

#include "common/exception/interrupt.h"
#include "function/gds/gds_frontier.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

void PathsOutputs::beginWritingOutputsForDstNodesInTable(common::table_id_t tableID) {
    pathLengths->pinCurFrontierTableID(tableID);
    bfsGraph->pinTableID(tableID);
}

std::unique_ptr<common::ValueVector> GDSOutputWriter::createVector(const LogicalType& type,
    storage::MemoryManager* mm) {
    auto vector = std::make_unique<ValueVector>(type.copy(), mm);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(vector.get());
    return vector;
}

RJOutputWriter::RJOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
    processor::NodeOffsetMaskMap* outputNodeMask)
    : GDSOutputWriter{context, outputNodeMask}, rjOutputs{rjOutputs} {
    auto mm = context->getMemoryManager();
    srcNodeIDVector = createVector(LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector = createVector(LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->setValue<nodeID_t>(0, rjOutputs->sourceNodeID);
}

void RJOutputWriter::pinTableID(common::table_id_t tableID) {
    GDSOutputWriter::pinTableID(tableID);
    rjOutputs->beginWritingOutputsForDstNodesInTable(tableID);
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

static ParentList* getTop(const std::vector<ParentList*>& path) {
    return path[path.size() - 1];
}

void PathsOutputWriter::write(processor::FactorizedTable& fTable, nodeID_t dstNodeID,
    GDSOutputCounter* counter) {
    auto output = rjOutputs->ptrCast<PathsOutputs>();
    auto& bfsGraph = *output->bfsGraph;
    auto sourceNodeID = output->sourceNodeID;
    dstNodeIDVector->setValue<common::nodeID_t>(0, dstNodeID);
    auto firstParent = findFirstParent(dstNodeID.offset, bfsGraph);
    if (firstParent == nullptr) {
        if (sourceNodeID == dstNodeID && info.lowerBound == 0) {
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
    if (!info.hasNodeMask() && info.semantic == common::PathSemantic::WALK) {
        // Fast path when there is no node predicate or semantic check
        while (!curPath.empty()) {
            if (context->interrupted()) {
                throw InterruptException{};
            }
            auto top = curPath[curPath.size() - 1];
            auto topNodeID = top->getNodeID();
            if (top->getIter() == 1) {
                writePath(curPath);
                fTable.append(vectors);
                if (counter != nullptr) {
                    counter->increase(1);
                    if (counter->exceedLimit()) {
                        return;
                    }
                }
                backtracking = true;
            }
            if (backtracking) {
                auto next = getTop(curPath)->getNextPtr();
                if (isNextViable(next, curPath)) {
                    curPath[curPath.size() - 1] = next;
                    if (curPath.size() == 1) {
                        setLength(lengthVector.get(), curPath[0]->getIter());
                    }
                    backtracking = false;
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
    while (!curPath.empty()) {
        if (context->interrupted()) {
            throw InterruptException{};
        }
        if (getTop(curPath)->getIter() == 1) {
            writePath(curPath);
            fTable.append(vectors);
            if (counter != nullptr) {
                counter->increase(1);
                if (counter->exceedLimit()) {
                    return;
                }
            }
            backtracking = true;
        }
        if (backtracking) {
            auto next = getTop(curPath)->getNextPtr();
            while (true) {
                if (!isNextViable(next, curPath)) {
                    curPath.pop_back();
                    break;
                }
                // Further check next against path node mask (predicate).
                if (!checkPathNodeMask(next) || !checkReplaceTopSemantic(curPath, next)) {
                    next = next->getNextPtr();
                    continue;
                }
                // Next is a valid path element. Push into stack and switch to forward track.
                curPath[curPath.size() - 1] = next;
                if (curPath.size() == 1) {
                    setLength(lengthVector.get(), curPath[0]->getIter());
                }
                backtracking = false;
                break;
            }
        } else {
            auto top = getTop(curPath);
            auto parent = bfsGraph.getParentListHead(top->getNodeID());
            while (true) {
                if (parent == nullptr) {
                    // No more forward tracking candidates. Switch to backward tracking.
                    backtracking = true;
                    break;
                }
                if (parent->getIter() == top->getIter() - 1 && checkPathNodeMask(parent) &&
                    checkAppendSemantic(curPath, parent)) {
                    // A forward tracking candidate should decrease the iteration by one and also
                    // pass node predicate checking.
                    curPath.push_back(parent);
                    backtracking = false;
                    break;
                }
                parent = parent->getNextPtr();
            }
        }
    }
    return;
}

ParentList* PathsOutputWriter::findFirstParent(offset_t dstOffset, BFSGraph& bfsGraph) const {
    auto result = bfsGraph.getParentListHead(dstOffset);
    if (!info.hasNodeMask() && info.semantic == PathSemantic::WALK) {
        // Fast path when there is no node predicate or semantic check
        return result;
    }
    while (result) {
        // A valid parent should
        // (1) satisfies path node semi mask (i.e. path node predicate)
        // (2) since first parent has the largest iteration number which decides path length, we
        //     also need to check if path length is greater than lower bound.
        if (checkPathNodeMask(result) && result->getIter() >= info.lowerBound) {
            break;
        }
        result = result->getNextPtr();
    }
    return result;
}

// This code checks if we should switch from backtracking to forward-tracking, i.e.,
// moving forward in the DFS logic to find paths. We switch from backtracking if:
bool PathsOutputWriter::isNextViable(ParentList* next, const std::vector<ParentList*>& path) const {
    if (next == nullptr) {
        return false;
    }
    auto nextIter = next->getIter();
    // (1) if this is the first element in the stack (curPath.size() == 1), i.e., we
    // are enumerating the parents of the destination, then we should switch to
    // forward-tracking if the next parent has visited the destination at a length
    // that's greater than or equal to the lower bound of the recursive join. Otherwise, we would
    // enumerate paths that are smaller than the lower bound from the start element, so we can stop
    // here.; OR
    if (path.size() == 1) {
        return nextIter >= info.lowerBound;
    }
    // (2) if this is not the first element in the stack, i.e., then we should switch
    // to forward tracking only if the next parent of the top node in the stack has the
    // same iter value as the current parent. That's because the levels/iter need to
    // decrease by 1 each time we add a new node in the stack.
    if (nextIter == getTop(path)->getIter()) {
        return true;
    }
    return false;
}

bool PathsOutputWriter::checkPathNodeMask(ParentList* element) const {
    if (!info.hasNodeMask() || element->getIter() == 1) {
        return true;
    }
    return info.pathNodeMask->valid(element->getNodeID());
}

bool PathsOutputWriter::checkAppendSemantic(const std::vector<ParentList*>& path,
    ParentList* candidate) const {
    switch (info.semantic) {
    case PathSemantic::WALK:
        return true;
    case PathSemantic::TRAIL:
        return isAppendTrail(path, candidate);
    case PathSemantic::ACYCLIC:
        return isAppendAcyclic(path, candidate);
    default:
        KU_UNREACHABLE;
    }
}

bool PathsOutputWriter::checkReplaceTopSemantic(const std::vector<ParentList*>& path,
    ParentList* candidate) const {
    switch (info.semantic) {
    case PathSemantic::WALK:
        return true;
    case PathSemantic::TRAIL:
        return isReplaceTopTrail(path, candidate);
    case PathSemantic::ACYCLIC:
        return isReplaceTopAcyclic(path, candidate);
    default:
        KU_UNREACHABLE;
    }
}

bool PathsOutputWriter::isAppendTrail(const std::vector<ParentList*>& path,
    ParentList* candidate) const {
    for (auto& element : path) {
        if (candidate->getEdgeID() == element->getEdgeID()) {
            return false;
        }
    }
    return true;
}

bool PathsOutputWriter::isAppendAcyclic(const std::vector<ParentList*>& path,
    ParentList* candidate) const {
    // Skip dst for semantic checking
    for (auto i = 1u; i < path.size() - 1; ++i) {
        if (candidate->getNodeID() == path[i]->getNodeID()) {
            return false;
        }
    }
    return true;
}

bool PathsOutputWriter::isReplaceTopTrail(const std::vector<ParentList*>& path,
    ParentList* candidate) const {
    for (auto i = 0u; i < path.size() - 1; ++i) {
        if (candidate->getEdgeID() == path[i]->getEdgeID()) {
            return false;
        }
    }
    return true;
}

bool PathsOutputWriter::isReplaceTopAcyclic(const std::vector<ParentList*>& path,
    ParentList* candidate) const {
    // Skip dst for semantic checking
    for (auto i = 1u; i < path.size() - 1; ++i) {
        if (candidate->getNodeID() == path[i]->getNodeID()) {
            return false;
        }
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
