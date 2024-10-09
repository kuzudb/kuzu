#include "processor/operator/recursive_extend/path_property_probe.h"

#include "function/hash/hash_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void PathPropertyProbe::initLocalStateInternal(ResultSet* /*resultSet_*/,
    ExecutionContext* /*context*/) {
    localState = PathPropertyProbeLocalState();
    auto pathVector = resultSet->getValueVector(info.pathPos);
    pathNodesVector = StructVector::getFieldVectorRaw(*pathVector, InternalKeyword::NODES);
    pathRelsVector = StructVector::getFieldVectorRaw(*pathVector, InternalKeyword::RELS);
    auto nodesDataVector = ListVector::getDataVector(pathNodesVector);
    auto relsDataVector = ListVector::getDataVector(pathRelsVector);
    pathNodeIDsDataVector = StructVector::getFieldVectorRaw(*nodesDataVector, InternalKeyword::ID);
    pathNodeLabelsDataVector =
        StructVector::getFieldVectorRaw(*nodesDataVector, InternalKeyword::LABEL);
    pathRelIDsDataVector = StructVector::getFieldVectorRaw(*relsDataVector, InternalKeyword::ID);
    pathRelLabelsDataVector =
        StructVector::getFieldVectorRaw(*relsDataVector, InternalKeyword::LABEL);
    pathSrcNodeIDsDataVector =
        StructVector::getFieldVectorRaw(*relsDataVector, InternalKeyword::SRC);
    pathDstNodeIDsDataVector =
        StructVector::getFieldVectorRaw(*relsDataVector, InternalKeyword::DST);
    for (auto fieldIdx : info.nodeFieldIndices) {
        pathNodesPropertyDataVectors.push_back(
            StructVector::getFieldVector(nodesDataVector, fieldIdx).get());
    }
    for (auto fieldIdx : info.relFieldIndices) {
        pathRelsPropertyDataVectors.push_back(
            StructVector::getFieldVector(relsDataVector, fieldIdx).get());
    }
    if (info.srcNodeIDPos.isValid()) {
        inputSrcNodeIDVector = resultSet->getValueVector(info.srcNodeIDPos).get();
        inputDstNodeIDVector = resultSet->getValueVector(info.dstNodeIDPos).get();
        inputNodeIDsVector = resultSet->getValueVector(info.inputNodeIDsPos).get();
        inputRelIDsVector = resultSet->getValueVector(info.inputEdgeIDsPos).get();
        if (info.directionPos.isValid()) {
            inputDirectionVector = resultSet->getValueVector(info.directionPos).get();
        }
    }
}

static void copyListEntry(const ValueVector& srcVector, ValueVector* dstVector) {
    auto& selVector = srcVector.state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); ++i) {
        auto pos = selVector[i];
        auto entry = srcVector.getValue<list_entry_t>(pos);
        ListVector::addList(dstVector, entry.size);
        dstVector->setValue(pos, entry);
    }
}

static void copyInternalID(ValueVector* srcVector, ValueVector* dstIDVector,
    ValueVector* dstLabelVector,
    const std::unordered_map<common::table_id_t, std::string>& tableIDToName) {
    auto srcDataVector = ListVector::getDataVector(srcVector);
    for (auto i = 0u; i < ListVector::getDataVectorSize(srcVector); ++i) {
        auto id = srcDataVector->getValue<internalID_t>(i);
        dstIDVector->setValue(i, id);
        StringVector::addString(dstLabelVector, i, tableIDToName.at(id.tableID));
    }
}

static void writeSrcDstNodeIDs(nodeID_t fromID, nodeID_t toID, ValueVector* directionDataVector,
    ValueVector* srcNodeIDsDataVector, ValueVector* dstNodeIDsDataVector, sel_t pos) {
    if (directionDataVector->getValue<bool>(pos)) {
        srcNodeIDsDataVector->setValue(pos, fromID);
        dstNodeIDsDataVector->setValue(pos, toID);
    } else {
        srcNodeIDsDataVector->setValue(pos, toID);
        dstNodeIDsDataVector->setValue(pos, fromID);
    }
}

bool PathPropertyProbe::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto sizeProbed = 0u;
    // Copy node IDs
    if (inputNodeIDsVector != nullptr) {
        copyListEntry(*inputNodeIDsVector, pathNodesVector);
        copyInternalID(inputNodeIDsVector, pathNodeIDsDataVector, pathNodeLabelsDataVector,
            info.tableIDToName);
    }
    // Scan node properties
    if (sharedState->nodeHashTableState != nullptr) {
        auto nodeHashTable = sharedState->nodeHashTableState->getHashTable();
        auto nodeDataSize = ListVector::getDataVectorSize(pathNodesVector);
        while (sizeProbed < nodeDataSize) {
            auto sizeToProbe =
                std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, nodeDataSize - sizeProbed);
            probe(nodeHashTable, sizeProbed, sizeToProbe, pathNodeIDsDataVector,
                pathNodesPropertyDataVectors, info.nodeTableColumnIndices);
            sizeProbed += sizeToProbe;
        }
    }
    // Copy rel IDs
    if (inputRelIDsVector != nullptr) {
        copyListEntry(*inputRelIDsVector, pathRelsVector);
        copyInternalID(inputRelIDsVector, pathRelIDsDataVector, pathRelLabelsDataVector,
            info.tableIDToName);
    }
    // Scan rel property
    if (sharedState->relHashTableState != nullptr) {
        auto relHashTable = sharedState->relHashTableState->getHashTable();
        auto relDataSize = ListVector::getDataVectorSize(pathRelsVector);
        sizeProbed = 0u;
        while (sizeProbed < relDataSize) {
            auto sizeToProbe =
                std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, relDataSize - sizeProbed);
            probe(relHashTable, sizeProbed, sizeToProbe, pathRelIDsDataVector,
                pathRelsPropertyDataVectors, info.relTableColumnIndices);
            sizeProbed += sizeToProbe;
        }
    }
    if (inputNodeIDsVector == nullptr || inputRelIDsVector == nullptr) {
        return true;
    }
    auto& selVector = inputNodeIDsVector->state->getSelVector();
    auto inputNodeIDsDataVector = ListVector::getDataVector(inputNodeIDsVector);
    // Copy rel src&dst IDs
    switch (info.pathSrcDstComputeInfo) {
    case PathSrcDstComputeInfo::ORDERED: {
        for (auto i = 0u; i < selVector.getSelSize(); ++i) {
            auto srcNodeID = inputSrcNodeIDVector->getValue<nodeID_t>(i);
            auto dstNodeID = inputDstNodeIDVector->getValue<nodeID_t>(i);
            auto nodeListEntry = inputNodeIDsVector->getValue<list_entry_t>(i);
            auto relListEntry = inputRelIDsVector->getValue<list_entry_t>(i);
            if (relListEntry.size == 0) {
                continue;
            }
            for (auto j = 0u; j < nodeListEntry.size; ++j) {
                auto id = inputNodeIDsDataVector->getValue<nodeID_t>(nodeListEntry.offset + j);
                pathSrcNodeIDsDataVector->setValue(relListEntry.offset + j + 1, id);
                pathDstNodeIDsDataVector->setValue(relListEntry.offset + j, id);
            }
            pathSrcNodeIDsDataVector->setValue(relListEntry.offset, srcNodeID);
            pathDstNodeIDsDataVector->setValue(relListEntry.offset + relListEntry.size - 1,
                dstNodeID);
        }
    } break;
    case PathSrcDstComputeInfo::FLIP: {
        for (auto i = 0u; i < selVector.getSelSize(); ++i) {
            auto srcNodeID = inputSrcNodeIDVector->getValue<nodeID_t>(i);
            auto dstNodeID = inputDstNodeIDVector->getValue<nodeID_t>(i);
            auto nodeListEntry = inputNodeIDsVector->getValue<list_entry_t>(i);
            auto relListEntry = inputRelIDsVector->getValue<list_entry_t>(i);
            if (relListEntry.size == 0) {
                continue;
            }
            for (auto j = 0u; j < nodeListEntry.size; ++j) {
                auto id = inputNodeIDsDataVector->getValue<nodeID_t>(nodeListEntry.offset + j);
                pathSrcNodeIDsDataVector->setValue(relListEntry.offset + j + 1, id);
                pathDstNodeIDsDataVector->setValue(relListEntry.offset + j, id);
            }
            pathSrcNodeIDsDataVector->setValue(relListEntry.offset, srcNodeID);
            pathDstNodeIDsDataVector->setValue(relListEntry.offset + relListEntry.size - 1,
                dstNodeID);
        }
    } break;
    case PathSrcDstComputeInfo::RUNTIME_CHECK: {
        auto directionDataVector = ListVector::getDataVector(inputDirectionVector);
        for (auto i = 0u; i < selVector.getSelSize(); ++i) {
            auto srcNodeID = inputSrcNodeIDVector->getValue<nodeID_t>(i);
            auto dstNodeID = inputDstNodeIDVector->getValue<nodeID_t>(i);
            auto nodeListEntry = inputNodeIDsVector->getValue<list_entry_t>(i);
            auto relListEntry = inputRelIDsVector->getValue<list_entry_t>(i);
            if (relListEntry.size == 0) {
                continue;
            }
            if (nodeListEntry.size == 0) {
                KU_ASSERT(relListEntry.size == 1);
                if (directionDataVector->getValue<bool>(relListEntry.offset)) {
                    pathSrcNodeIDsDataVector->setValue(relListEntry.offset, srcNodeID);
                    pathDstNodeIDsDataVector->setValue(relListEntry.offset, dstNodeID);
                } else {
                    pathSrcNodeIDsDataVector->setValue(relListEntry.offset, dstNodeID);
                    pathDstNodeIDsDataVector->setValue(relListEntry.offset, srcNodeID);
                }
                continue;
            }
            for (auto j = 0u; j < nodeListEntry.size - 1; ++j) {
                auto from = inputNodeIDsDataVector->getValue<nodeID_t>(nodeListEntry.offset + j);
                auto to = inputNodeIDsDataVector->getValue<nodeID_t>(nodeListEntry.offset + j + 1);
                writeSrcDstNodeIDs(from, to, directionDataVector, pathSrcNodeIDsDataVector,
                    pathDstNodeIDsDataVector, relListEntry.offset + j + 1);
            }
            writeSrcDstNodeIDs(srcNodeID,
                inputNodeIDsDataVector->getValue<nodeID_t>(nodeListEntry.offset),
                directionDataVector, pathSrcNodeIDsDataVector, pathDstNodeIDsDataVector,
                relListEntry.offset);
            writeSrcDstNodeIDs(inputNodeIDsDataVector->getValue<nodeID_t>(
                                   nodeListEntry.offset + nodeListEntry.size - 1),
                dstNodeID, directionDataVector, pathSrcNodeIDsDataVector, pathDstNodeIDsDataVector,
                relListEntry.offset + relListEntry.size - 1);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    return true;
}

void PathPropertyProbe::probe(kuzu::processor::JoinHashTable* hashTable, uint64_t sizeProbed,
    uint64_t sizeToProbe, ValueVector* idVector, const std::vector<ValueVector*>& propertyVectors,
    const std::vector<ft_col_idx_t>& colIndicesToScan) const {
    // Hash
    for (auto i = 0u; i < sizeToProbe; ++i) {
        function::Hash::operation(idVector->getValue<internalID_t>(sizeProbed + i),
            localState.hashes[i]);
    }
    // Probe hash
    for (auto i = 0u; i < sizeToProbe; ++i) {
        localState.probedTuples[i] = hashTable->getTupleForHash(localState.hashes[i]);
    }
    // Match value
    for (auto i = 0u; i < sizeToProbe; ++i) {
        while (localState.probedTuples[i]) {
            auto currentTuple = localState.probedTuples[i];
            if (*(internalID_t*)currentTuple == idVector->getValue<internalID_t>(sizeProbed + i)) {
                localState.matchedTuples[i] = currentTuple;
                break;
            }
            localState.probedTuples[i] = *hashTable->getPrevTuple(currentTuple);
        }
        KU_ASSERT(localState.matchedTuples[i] != nullptr);
    }
    // Scan table
    auto factorizedTable = hashTable->getFactorizedTable();
    for (auto i = 0u; i < sizeToProbe; ++i) {
        auto tuple = localState.matchedTuples[i];
        for (auto j = 0u; j < propertyVectors.size(); ++j) {
            auto propertyVector = propertyVectors[j];
            auto colIdx = colIndicesToScan[j];
            factorizedTable->readFlatColToFlatVector(tuple, colIdx, *propertyVector,
                sizeProbed + i);
        }
    }
}

} // namespace processor
} // namespace kuzu
