#include "processor/operator/copy_from/copy_rdf.h"

#include <utility>
#include <vector>

#include "common/constants.h"
#include "common/string_utils.h"
#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"
#include "storage/copier/rel_copy_executor.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

CopyRDFSharedState::CopyRDFSharedState(common::rdf_graph_id_t rdfGraphId,
    storage::NodeTable* resourcesNodeTable, storage::RelTable* triplesRelTable,
    storage::RelsStore* relsStore, catalog::Catalog* catalog, storage::WAL* wal,
    storage::MemoryManager* memoryManager, std::shared_ptr<ReadRDFSharedState> readerSharedState)
    : rdfGraphId{rdfGraphId}, resourcesNodeTable{resourcesNodeTable},
      triplesRelTable{triplesRelTable}, relsStore{relsStore}, catalog{catalog}, wal{wal},
      hasInitialized{false}, numOfTriples{0}, currentOffset{0}, currentNodeGroupIdx{0},
      readerSharedState{std::move(readerSharedState)} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            common::LogicalTypeUtils::getRowLayoutSize(
                common::LogicalType{common::LogicalTypeID::STRING})));
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));

    fwdRelData = std::make_unique<DirectedInMemRelLists>();
    bwdRelData = std::make_unique<DirectedInMemRelLists>();

    resourcesNodeTableSchema =
        catalog->getReadOnlyVersion()->getNodeTableSchema(resourcesNodeTable->getTableID());
    triplesRelTableSchema =
        catalog->getReadOnlyVersion()->getRelTableSchema(triplesRelTable->getRelTableID());

    subjectSelectionVector = std::make_unique<common::SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    predicateSelectionVector = std::make_unique<common::SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    objectSelectionVector = std::make_unique<common::SelectionVector>(DEFAULT_VECTOR_CAPACITY);
}

void CopyRDFSharedState::initializePrimaryKey() {
    pkHashIndex = std::make_unique<storage::HashIndexBuilder<common::ku_string_t>>(
        storage::StorageUtils::getNodeIndexFName(
            wal->getDirectory(), resourcesNodeTableSchema->tableID, common::DBFileType::ORIGINAL),
        *resourcesNodeTableSchema->getPrimaryKey()->getDataType());

    // Initialize hash index with 50% of approx number of iris.
    offset_t initialHashCapacity = readerSharedState->getApproxNumberOfIRIs() * 0.50;
    pkHashIndex->bulkReserve(initialHashCapacity);
}

void CopyRDFSharedState::logCopyNodeWALRecord() {
    wal->logCopyRDFGraphRecord(resourcesNodeTable->getTableID(),
        resourcesNodeTable->getDataFH()->getNumPages(), triplesRelTable->getRelTableID());
    wal->flushAllPages();
}

void CopyRDF::executeInternal(kuzu::processor::ExecutionContext* context) {
    // build node table and initialize rel lists
    copyNodeTableAndInitializeRelLists(context);

    // build rel list
    while (children[0]->getNextTuple(context)) {
        auto offsetVector = localState->offsetVector;
        auto startOffset =
            offsetVector->getValue<int64_t>(offsetVector->state->selVector->selectedPositions[0]);
        auto endOffset =
            offsetVector->getValue<int64_t>(offsetVector->state->selVector->selectedPositions[1]);
        auto length = endOffset - startOffset;
        lookupAndBuildOffset(length);

        copyRelLists(startOffset, length, sharedState->fwdRelData.get(), localState->subjectOffsets,
            localState->objectOffsets, localState->predicateOffsets);
        copyRelLists(startOffset, length, sharedState->bwdRelData.get(), localState->objectOffsets,
            localState->subjectOffsets, localState->predicateOffsets);
    }
}

void CopyRDF::copyRelLists(const common::offset_t& startOffset, const common::offset_t& length,
    storage::DirectedInMemRelLists* directedInMemRelLists,
    const std::vector<offset_t>& boundOffsets, const std::vector<offset_t>& adjOffsets,
    const std::vector<offset_t>& propertyValues) {
    std::vector<offset_t> posInRelLists(length), relIDs(length);
    std::vector<nodeID_t> predicateOffsets(length);
    for (int i = 0; i < length; i++) {
        posInRelLists[i] = InMemListsUtils::decrementListSize(
            *directedInMemRelLists->relListsSizes, boundOffsets[i], 1);
        directedInMemRelLists->adjList->setValue(
            boundOffsets[i], posInRelLists[i], (uint8_t*)&adjOffsets[i]);
        relIDs[i] = startOffset + i;
        predicateOffsets[i] =
            nodeID_t(propertyValues[i], sharedState->resourcesNodeTable->getTableID());
    }

    for (int i = 0; i < length; i++) {
        directedInMemRelLists->propertyLists.at(0)->setValue(
            boundOffsets[i], posInRelLists[i], (uint8_t*)&relIDs[i]);
        directedInMemRelLists->propertyLists.at(1)->setValue(
            boundOffsets[i], posInRelLists[i], (uint8_t*)&predicateOffsets[i]);
    }
}

inline void CopyRDF::lookupAndBuildOffset(const common::offset_t& length) {
    for (int i = 0; i < length; i++) {
        lookupHashIndex(localState->subjectVector, i, localState->subjectOffsets[i]);
        lookupHashIndex(localState->predicateVector, i, localState->predicateOffsets[i]);
        lookupHashIndex(localState->objectVector, i, localState->objectOffsets[i]);
    }
}

inline void CopyRDF::lookupHashIndex(
    const std::shared_ptr<common::ValueVector>& vector, int pos, common::offset_t& offset) {
    auto uriString = vector->getValue<common::ku_string_t>(pos).getAsString();
    if (!sharedState->pkHashIndex->lookup(uriString.c_str(), uriString.length(), offset)) {
        throw common::CopyException("Unable to find iri " + uriString +
                                    " in the hash index while creating its relationship.");
    }
}

void CopyRDF::copyNodeTableAndInitializeRelLists(kuzu::processor::ExecutionContext* context) {
    std::unique_lock xLck{sharedState->mtx};
    if (sharedState->hasInitialized) {
        return;
    }

    // Log WAL
    sharedState->logCopyNodeWALRecord();

    // declare fwd and bwd relListSizes
    std::vector<uint64_t> fwdRelListSizes, bwdRelListSizes;

    // build node table
    while (children[0]->getNextTuple(context)) {
        auto offsetVector = localState->offsetVector;
        auto startOffset =
            offsetVector->getValue<int64_t>(offsetVector->state->selVector->selectedPositions[0]);
        auto endOffset =
            offsetVector->getValue<int64_t>(offsetVector->state->selVector->selectedPositions[1]);
        auto length = endOffset - startOffset;
        populateHashIndexAndColumnChunk(length);
        countRelListsSize(fwdRelListSizes, bwdRelListSizes, length);
        sharedState->numOfTriples += length;
    }

    // Flush the last local node group if it has some nodes
    if (sharedState->nodeGroup->getNumNodes() > 0) {
        auto nodeGroupIdx = sharedState->getNextNodeGroupIdx();
        writeAndResetNodeGroup(
            nodeGroupIdx, sharedState->resourcesNodeTable, sharedState->nodeGroup.get());
    }

    // initialize rel table
    initializeDirectedInMemRelData(FWD, sharedState->fwdRelData);
    initializeDirectedInMemRelData(BWD, sharedState->bwdRelData);

    // build fwd and bwd rel list headers and metadata
    for (int i = 0; i < sharedState->currentOffset; i++) {
        sharedState->fwdRelData->relListsSizes->at(i).store(fwdRelListSizes[i]);
        sharedState->bwdRelData->relListsSizes->at(i).store(bwdRelListSizes[i]);
    }

    buildRelListsHeaders(sharedState->fwdRelData->adjList->getListHeadersBuilder().get(),
        *sharedState->fwdRelData->relListsSizes);
    buildRelListsMetadata(sharedState->fwdRelData.get());

    buildRelListsHeaders(sharedState->bwdRelData->adjList->getListHeadersBuilder().get(),
        *sharedState->bwdRelData->relListsSizes);
    buildRelListsMetadata(sharedState->bwdRelData.get());

    // Reset reader shared state
    sharedState->readerSharedState->reset();

    // Set the initialized flag
    sharedState->hasInitialized = true;
}

void CopyRDF::buildRelListsHeaders(
    ListHeadersBuilder* relListHeadersBuilder, const atomic_uint64_vec_t& relListsSizes) {
    auto numBoundNodes = relListHeadersBuilder->getNumValues();
    auto numChunks = StorageUtils::getNumChunks(numBoundNodes);
    offset_t nodeOffset = 0;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        offset_t chunkNodeOffset = nodeOffset;
        auto numNodesInChunk = std::min(
            (offset_t)ListsMetadataConstants::LISTS_CHUNK_SIZE, numBoundNodes - nodeOffset);
        csr_offset_t csrOffset = relListsSizes[chunkNodeOffset].load(std::memory_order_relaxed);
        for (auto i = 1u; i < numNodesInChunk; i++) {
            auto currNodeOffset = chunkNodeOffset + i;
            auto numElementsInList = relListsSizes[currNodeOffset].load(std::memory_order_relaxed);
            relListHeadersBuilder->setCSROffset(currNodeOffset, csrOffset);
            csrOffset += numElementsInList;
        }
        relListHeadersBuilder->setCSROffset(chunkNodeOffset + numNodesInChunk, csrOffset);
        nodeOffset += numNodesInChunk;
    }
}

void CopyRDF::buildRelListsMetadata(DirectedInMemRelLists* directedInMemRelList) {
    auto relListHeaders = directedInMemRelList->adjList->getListHeadersBuilder().get();
    buildRelListsMetadata(directedInMemRelList->adjList.get(), relListHeaders);
    for (auto& [_, propertyRelLists] : directedInMemRelList->propertyLists) {
        buildRelListsMetadata(propertyRelLists.get(), relListHeaders);
    }
}

void CopyRDF::buildRelListsMetadata(InMemLists* relLists, ListHeadersBuilder* relListHeaders) {
    auto numBoundNodes = relListHeaders->getNumValues();
    auto numChunks = StorageUtils::getNumChunks(numBoundNodes);
    offset_t nodeOffset = 0;
    auto numValuesPerPage = relLists->getNumElementsInAPage();
    for (auto chunkIdx = 0u; chunkIdx < numChunks; chunkIdx++) {
        auto numPagesForChunk = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConstants::LISTS_CHUNK_SIZE, numBoundNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto numValuesInRelList = relListHeaders->getListSize(nodeOffset);
            while (numValuesInRelList + offsetInPage > numValuesPerPage) {
                numValuesInRelList -= (numValuesPerPage - offsetInPage);
                numPagesForChunk++;
                offsetInPage = 0u;
            }
            offsetInPage += numValuesInRelList;
            nodeOffset++;
        }
        if (0 != offsetInPage) {
            numPagesForChunk++;
        }
        relLists->getListsMetadataBuilder()->populateChunkPageList(
            chunkIdx, numPagesForChunk, relLists->inMemFile->getNumPages());
        relLists->inMemFile->addNewPages(numPagesForChunk);
    }
}

void CopyRDF::populateHashIndexAndColumnChunk(const common::offset_t& length) {
    common::offset_t start = sharedState->currentOffset;
    appendToHashIndex(localState->subjectVector.get(), length, localState->subjectOffsets,
        sharedState->subjectSelectionVector.get());
    appendToHashIndex(localState->predicateVector.get(), length, localState->predicateOffsets,
        sharedState->predicateSelectionVector.get());
    appendToHashIndex(localState->objectVector.get(), length, localState->objectOffsets,
        sharedState->objectSelectionVector.get());

    // It's okay to bulk reserve after inserting batch to hash index since we are initializing
    // large enough hash index based on the file size.
    sharedState->pkHashIndex->bulkReserveIfRequired(sharedState->currentOffset - start - 1);

    copyToNodeGroup(localState->subjectVector.get(), sharedState->subjectSelectionVector.get());
    copyToNodeGroup(localState->predicateVector.get(), sharedState->predicateSelectionVector.get());
    copyToNodeGroup(localState->objectVector.get(), sharedState->objectSelectionVector.get());
}

inline void CopyRDF::appendToHashIndex(common::ValueVector* vector, const common::offset_t& length,
    std::vector<offset_t>& offsets, SelectionVector* selectionVector) {
    auto selectionBuffer = selectionVector->getSelectedPositionsBuffer();
    uint16_t pos = 0;
    for (int i = 0; i < length; i++) {
        auto uriString = vector->getValue<common::ku_string_t>(i).getAsString();
        if (sharedState->pkHashIndex->lookup(uriString.c_str(), uriString.length(), offsets[i])) {
            continue;
        }
        offsets[i] = sharedState->currentOffset++;
        sharedState->pkHashIndex->append(uriString.c_str(), uriString.length(), offsets[i]);
        selectionBuffer[pos++] = i;
    }
    selectionVector->selectedSize = pos;
}

inline void CopyRDF::copyToNodeGroup(
    common::ValueVector* vector, SelectionVector* selectionVector) {
    auto numTuplesToAppend = selectionVector->selectedSize;
    uint64_t numAppendedTuples = 0;
    while (numAppendedTuples < numTuplesToAppend) {
        numAppendedTuples += sharedState->nodeGroup->appendStringVector(
            vector, selectionVector, 0, numAppendedTuples, numTuplesToAppend);
        if (sharedState->nodeGroup->isFull()) {
            auto nodeGroupIdx = sharedState->getNextNodeGroupIdx();
            writeAndResetNodeGroup(
                nodeGroupIdx, sharedState->resourcesNodeTable, sharedState->nodeGroup.get());
        }
    }
}

void CopyRDF::initializeDirectedInMemRelData(common::RelDataDirection direction,
    const std::unique_ptr<storage::DirectedInMemRelLists>& relLists) {
    offset_t numNodes = sharedState->pkHashIndex->getNumEntries();
    std::string outputDirectory = sharedState->wal->getDirectory();
    table_id_t relTableID = sharedState->triplesRelTable->getRelTableID();

    // Validate that triples rel is not of single multiplicity type
    assert(!sharedState->triplesRelTableSchema->isSingleMultiplicityInDirection(direction));

    relLists->adjList =
        std::make_unique<InMemAdjLists>(StorageUtils::getAdjListsFName(outputDirectory, relTableID,
                                            direction, DBFileType::ORIGINAL),
            numNodes);
    relLists->relListsSizes = std::make_unique<atomic_uint64_vec_t>(numNodes);
    for (auto i = 0u; i < sharedState->triplesRelTableSchema->getNumProperties(); ++i) {
        auto propertyID = sharedState->triplesRelTableSchema->properties[i]->getPropertyID();
        auto propertyDataType = sharedState->triplesRelTableSchema->properties[i]->getDataType();
        auto fName = StorageUtils::getRelPropertyListsFName(
            outputDirectory, relTableID, direction, propertyID, DBFileType::ORIGINAL);
        relLists->propertyLists.emplace(
            propertyID, InMemListsFactory::getInMemPropertyLists(fName, *propertyDataType, numNodes,
                            &localState->copyDesc, relLists->adjList->getListHeadersBuilder()));
    }
}

void CopyRDF::countRelListsSize(std::vector<std::uint64_t>& fwdRelListSizes,
    std::vector<std::uint64_t>& bwdRelListSizes, const common::offset_t& length) {
    fwdRelListSizes.resize(sharedState->currentOffset);
    bwdRelListSizes.resize(sharedState->currentOffset);
    for (int i = 0; i < length; i++) {
        auto subjectOffset = localState->subjectOffsets[i];
        auto objectOffset = localState->objectOffsets[i];
        fwdRelListSizes[subjectOffset] += 1;
        bwdRelListSizes[objectOffset] += 1;
    }
}

void CopyRDF::finalize(kuzu::processor::ExecutionContext* context) {
    std::uint64_t numUniqueIris = sharedState->pkHashIndex->getNumEntries();
    if (sharedState->pkHashIndex) {
        sharedState->pkHashIndex->flush();
    }

    std::unordered_set<table_id_t> connectedRelTableIDs;
    connectedRelTableIDs.insert(
        sharedState->resourcesNodeTableSchema->getFwdRelTableIDSet().begin(),
        sharedState->resourcesNodeTableSchema->getFwdRelTableIDSet().end());
    connectedRelTableIDs.insert(
        sharedState->resourcesNodeTableSchema->getBwdRelTableIDSet().begin(),
        sharedState->resourcesNodeTableSchema->getBwdRelTableIDSet().end());
    for (auto relTableID : connectedRelTableIDs) {
        sharedState->relsStore->getRelTable(relTableID)
            ->batchInitEmptyRelsForNewNodes(relTableID, numUniqueIris);
    }
    sharedState->resourcesNodeTable->getNodeStatisticsAndDeletedIDs()->setNumTuplesForTable(
        sharedState->resourcesNodeTable->getTableID(), numUniqueIris);

    sharedState->fwdRelData->adjList->saveToFile();
    for (auto& [_, relLists] : sharedState->fwdRelData->propertyLists) {
        relLists->saveToFile();
    }
    sharedState->bwdRelData->adjList->saveToFile();
    for (auto& [_, relLists] : sharedState->bwdRelData->propertyLists) {
        relLists->saveToFile();
    }
    sharedState->relsStore->getRelsStatistics().setNumTuplesForTable(
        sharedState->triplesRelTable->getRelTableID(), sharedState->numOfTriples);

    auto outputMsg = common::StringUtils::string_format(
        "{} number of triples have been copied to the rdf graph: {}.", sharedState->numOfTriples,
        sharedState->catalog->getReadOnlyVersion()
            ->getRDFGraphSchema(sharedState->rdfGraphId)
            ->getRDFGraphName()
            .c_str());
    FactorizedTableUtils::appendStringToTable(
        sharedState->fTable.get(), outputMsg, context->memoryManager);
}

void CopyRDF::writeAndResetNodeGroup(common::node_group_idx_t nodeGroupIdx,
    storage::NodeTable* table, storage::NodeGroup* nodeGroup) {
    nodeGroup->setNodeGroupIdx(nodeGroupIdx);
    table->append(nodeGroup);
    nodeGroup->resetToEmpty();
}

} // namespace processor
} // namespace kuzu
