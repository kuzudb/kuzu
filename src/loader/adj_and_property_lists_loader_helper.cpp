#include "src/loader/include/adj_and_property_lists_loader_helper.h"

namespace graphflow {
namespace loader {

AdjAndPropertyListsLoaderHelper::AdjAndPropertyListsLoaderHelper(RelLabelDescription& description,
    ThreadPool& threadPool, const Graph& graph, const Catalog& catalog,
    const string outputDirectory)
    : logger{spdlog::get("loader")}, description{description},
      threadPool{threadPool}, graph{graph}, catalog{catalog}, outputDirectory{outputDirectory} {
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            dirLabelListSizes[dir].resize(catalog.getNodeLabelsCount());
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                dirLabelListSizes[dir][nodeLabel] =
                    make_unique<vector<atomic<uint64_t>>>(graph.getNumNodesPerLabel()[nodeLabel]);
            }
        }
    }
}

void AdjAndPropertyListsLoaderHelper::buildAdjListsHeadersAndListsMetadata() {
    for (auto& dir : DIRS) {
        dirLabelAdjListHeaders[dir].resize(catalog.getNodeLabelsCount());
        for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
            dirLabelAdjListHeaders[dir][nodeLabel].headers.resize(
                graph.getNumNodesPerLabel()[nodeLabel]);
        }
        dirLabelAdjListsMetadata[dir].resize(catalog.getNodeLabelsCount());
    }
    if (description.requirePropertyLists()) {
        for (auto& dir : DIRS) {
            dirLabelPropertyIdxPropertyListsMetadata[dir].resize(catalog.getNodeLabelsCount());
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                dirLabelPropertyIdxPropertyListsMetadata[dir][nodeLabel].resize(
                    description.propertyMap->size());
            }
        }
    }
    initAdjListHeaders();
    initAdjListsAndPropertyListsMetadata();
}

void AdjAndPropertyListsLoaderHelper::buildInMemStructures() {
    if (description.requirePropertyLists()) {
        buildInMemPropertyLists();
    }
    buildInMemAdjLists();
}

void AdjAndPropertyListsLoaderHelper::setRel(
    const uint64_t& pos, const Direction& dir, const vector<nodeID_t>& nodeIDs) {
    PageCursor cursor;
    auto header = dirLabelAdjListHeaders[dir][nodeIDs[dir].label].headers[nodeIDs[dir].offset];
    calculatePageCursor(header, pos,
        description.nodeIDCompressionSchemePerDir[dir].getNumTotalBytes(), nodeIDs[dir].offset,
        cursor, dirLabelAdjListsMetadata[dir][nodeIDs[dir].label]);
    dirLabelAdjLists[dir][nodeIDs[dir].label]->set(cursor, nodeIDs[!dir]);
}

void AdjAndPropertyListsLoaderHelper::setProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, const uint32_t& propertyIdx, const uint8_t* val,
    const DataType& type) {
    PageCursor cursor;
    for (auto& dir : DIRS) {
        auto header = dirLabelAdjListHeaders[dir][nodeIDs[dir].label].headers[nodeIDs[dir].offset];
        calculatePageCursor(header, pos[dir], getDataTypeSize(type), nodeIDs[dir].offset, cursor,
            dirLabelPropertyIdxPropertyListsMetadata[dir][nodeIDs[dir].label][propertyIdx]);
        dirLabelPropertyIdxPropertyLists[dir][nodeIDs[dir].label][propertyIdx]->set(cursor, val);
    }
}

void AdjAndPropertyListsLoaderHelper::setStringProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, const uint32_t& propertyIdx, const char* strVal,
    PageCursor& stringOverflowCursor) {
    PageCursor propertyListCursor;
    calculatePageCursor(
        dirLabelAdjListHeaders[FWD][nodeIDs[FWD].label].headers[nodeIDs[FWD].offset], pos[FWD],
        getDataTypeSize(STRING), nodeIDs[FWD].offset, propertyListCursor,
        dirLabelPropertyIdxPropertyListsMetadata[FWD][nodeIDs[FWD].label][propertyIdx]);
    auto encodedStrFwd = reinterpret_cast<gf_string_t*>(
        dirLabelPropertyIdxPropertyLists[FWD][nodeIDs[FWD].label][propertyIdx]->get(
            propertyListCursor));
    (*propertyIdxUnordStringOverflowPages)[propertyIdx]->set(
        strVal, stringOverflowCursor, encodedStrFwd);
    calculatePageCursor(
        dirLabelAdjListHeaders[BWD][nodeIDs[BWD].label].headers[nodeIDs[BWD].offset], pos[BWD],
        getDataTypeSize(STRING), nodeIDs[BWD].offset, propertyListCursor,
        dirLabelPropertyIdxPropertyListsMetadata[BWD][nodeIDs[BWD].label][propertyIdx]);
    auto encodedStrBwd = reinterpret_cast<gf_string_t*>(
        dirLabelPropertyIdxPropertyLists[BWD][nodeIDs[BWD].label][propertyIdx]->get(
            propertyListCursor));
    memcpy(encodedStrBwd, encodedStrFwd, getDataTypeSize(STRING));
}

void AdjAndPropertyListsLoaderHelper::sortOverflowStrings() {
    logger->debug("Ordering String Rel PropertyList.");
    dirLabelPropertyIdxStringOverflowPages =
        make_unique<dirLabelPropertyIdxStringOverflowPages_t>(2);
    for (auto& dir : DIRS) {
        (*dirLabelPropertyIdxStringOverflowPages)[dir].resize(catalog.getNodeLabelsCount());
        for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
            (*dirLabelPropertyIdxStringOverflowPages)[dir][nodeLabel].resize(
                description.propertyMap->size());
            for (auto property = description.propertyMap->begin();
                 property != description.propertyMap->end(); property++) {
                if (STRING == property->second.dataType) {
                    auto fname = RelsStore::getRelPropertyListsFname(
                        outputDirectory, description.label, nodeLabel, dir, property->first);
                    (*dirLabelPropertyIdxStringOverflowPages)[dir][nodeLabel][property->second
                                                                                  .idx] =
                        make_unique<InMemStringOverflowPages>(fname + ".ovf");
                    auto numNodes = graph.getNumNodesPerLabel()[nodeLabel];
                    auto numBuckets = numNodes / 256;
                    if (0 != numNodes % 256) {
                        numBuckets++;
                    }
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    auto idx = property->second.idx;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        threadPool.execute(sortOverflowStringsOfPropertyListsTask, offsetStart,
                            offsetEnd, dirLabelPropertyIdxPropertyLists[dir][nodeLabel][idx].get(),
                            &dirLabelAdjListHeaders[dir][nodeLabel],
                            &dirLabelPropertyIdxPropertyListsMetadata[dir][nodeLabel][idx],
                            (*propertyIdxUnordStringOverflowPages)[idx].get(),
                            (*dirLabelPropertyIdxStringOverflowPages)[dir][nodeLabel][idx].get(),
                            logger);
                    }
                }
            }
        }
    }
    threadPool.wait();
    propertyIdxUnordStringOverflowPages.reset();
    logger->debug("Done.");
}

void AdjAndPropertyListsLoaderHelper::saveToFile() {
    logger->debug("Writing AdjLists and Rel Property Lists to disk.");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                dirLabelAdjLists[dir][nodeLabel]->saveToFile();
                auto fname =
                    RelsStore::getAdjListsFname(outputDirectory, description.label, nodeLabel, dir);
                threadPool.execute([&](ListsMetadata& x, string fname) { x.saveToFile(fname); },
                    dirLabelAdjListsMetadata[dir][nodeLabel], fname);
                threadPool.execute([&](AdjListHeaders& x, string fname) { x.saveToFile(fname); },
                    dirLabelAdjListHeaders[dir][nodeLabel], fname);
            }
        }
    }
    if (description.requirePropertyLists()) {
        for (auto& dir : DIRS) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                for (auto property = description.propertyMap->begin();
                     property != description.propertyMap->end(); property++) {
                    auto idx = property->second.idx;
                    if (dirLabelPropertyIdxPropertyLists[dir][nodeLabel][idx]) {
                        threadPool.execute([&](InMemPropertyPages* x) { x->saveToFile(); },
                            dirLabelPropertyIdxPropertyLists[dir][nodeLabel][idx].get());
                        if (STRING == property->second.dataType) {
                            threadPool.execute(
                                [&](InMemStringOverflowPages* x) { x->saveToFile(); },
                                (*dirLabelPropertyIdxStringOverflowPages)[dir][nodeLabel][idx]
                                    .get());
                        }
                        auto fname = RelsStore::getRelPropertyListsFname(
                            outputDirectory, description.label, nodeLabel, dir, property->first);
                        threadPool.execute(
                            [&](ListsMetadata& x, string fname) { x.saveToFile(fname); },
                            dirLabelPropertyIdxPropertyListsMetadata[dir][nodeLabel][idx], fname);
                    }
                }
            }
        }
    }
    threadPool.wait();
    logger->debug("Done.");
}

void AdjAndPropertyListsLoaderHelper::initAdjListHeaders() {
    logger->debug("Initializing AdjListHeaders.");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            auto relSize = description.nodeIDCompressionSchemePerDir[dir].getNumTotalBytes();
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                threadPool.execute(calculateAdjListHeadersForIndexTask, dir, nodeLabel,
                    graph.getNumNodesPerLabel()[nodeLabel], PAGE_SIZE / relSize,
                    dirLabelListSizes[dir][nodeLabel].get(),
                    &dirLabelAdjListHeaders[dir][nodeLabel], logger);
            }
        }
    }
    threadPool.wait();
    logger->debug("Done.");
}

void AdjAndPropertyListsLoaderHelper::initAdjListsAndPropertyListsMetadata() {
    logger->debug("Initializing AdjLists and PropertyLists Metadata.");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            auto numPerPage =
                PAGE_SIZE / description.nodeIDCompressionSchemePerDir[dir].getNumTotalBytes();
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                threadPool.execute(calculateListsMetadataForListsTask,
                    graph.getNumNodesPerLabel()[nodeLabel], numPerPage,
                    dirLabelListSizes[dir][nodeLabel].get(),
                    &dirLabelAdjListHeaders[dir][nodeLabel],
                    &dirLabelAdjListsMetadata[dir][nodeLabel], logger);
            }
        }
    }
    if (description.requirePropertyLists()) {
        for (auto& dir : DIRS) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                auto listsSizes = dirLabelListSizes[dir][nodeLabel].get();
                auto numNodeOffsets = graph.getNumNodesPerLabel()[nodeLabel];
                for (auto property = description.propertyMap->begin();
                     property != description.propertyMap->end(); property++) {
                    auto idx = property->second.idx;
                    auto numPerPage = PAGE_SIZE / getDataTypeSize(property->second.dataType);
                    threadPool.execute(calculateListsMetadataForListsTask, numNodeOffsets,
                        numPerPage, listsSizes, &dirLabelAdjListHeaders[dir][nodeLabel],
                        &dirLabelPropertyIdxPropertyListsMetadata[dir][nodeLabel][idx], logger);
                }
            }
        }
    }
    threadPool.wait();
    logger->debug("Done.");
}

void AdjAndPropertyListsLoaderHelper::buildInMemAdjLists() {
    logger->debug("Creating InMemAdjLists.");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            dirLabelAdjLists[dir].resize(catalog.getNodeLabelsCount());
            for (auto boundNodeLabel : description.nodeLabelsPerDir[dir]) {
                auto fname = RelsStore::getAdjListsFname(
                    outputDirectory, description.label, boundNodeLabel, dir);
                dirLabelAdjLists[dir][boundNodeLabel] = make_unique<InMemAdjPages>(fname,
                    dirLabelAdjListsMetadata[dir][boundNodeLabel].numPages,
                    description.nodeIDCompressionSchemePerDir[dir].getNumBytesForLabel(),
                    description.nodeIDCompressionSchemePerDir[dir].getNumBytesForOffset());
            }
        }
    }
    logger->debug("Done.");
}

void AdjAndPropertyListsLoaderHelper::buildInMemPropertyLists() {
    logger->debug("Creating InMemPropertyLists.");
    for (auto& dir : DIRS) {
        dirLabelPropertyIdxPropertyLists[dir].resize(catalog.getNodeLabelsCount());
        for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
            dirLabelPropertyIdxPropertyLists[dir][nodeLabel].resize(
                description.propertyMap->size());
            for (auto property = description.propertyMap->begin();
                 property != description.propertyMap->end(); property++) {
                auto idx = property->second.idx;
                auto fname = RelsStore::getRelPropertyListsFname(
                    outputDirectory, description.label, nodeLabel, dir, property->first);
                dirLabelPropertyIdxPropertyLists[dir][nodeLabel][idx] =
                    make_unique<InMemPropertyPages>(fname,
                        dirLabelPropertyIdxPropertyListsMetadata[dir][nodeLabel][idx].numPages,
                        getDataTypeSize(property->second.dataType));
            }
        }
    }
    propertyIdxUnordStringOverflowPages =
        make_unique<vector<unique_ptr<InMemStringOverflowPages>>>(description.propertyMap->size());
    for (auto property = description.propertyMap->begin();
         property != description.propertyMap->end(); property++) {
        if (STRING == property->second.dataType) {
            (*propertyIdxUnordStringOverflowPages)[property->second.idx] =
                make_unique<InMemStringOverflowPages>();
        }
    }
    logger->debug("Done.");
}

void AdjAndPropertyListsLoaderHelper::calculatePageCursor(const uint32_t& header,
    const uint64_t& reversePos, const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset,
    PageCursor& cursor, ListsMetadata& metadata) {
    auto numElementsInAPage = PAGE_SIZE / numBytesPerElement;
    if (AdjListHeaders::isALargeAdjList(header)) {
        auto lAdjListIdx = AdjListHeaders::getLargeAdjListIdx(header);
        auto pos = metadata.largeListsPagesMap[lAdjListIdx][0] - reversePos;
        cursor.offset = numBytesPerElement * (pos % numElementsInAPage);
        cursor.idx = metadata.largeListsPagesMap[lAdjListIdx][1 + (pos / numElementsInAPage)];
        return;
    }
    auto chunkId = nodeOffset / BaseLists::LISTS_CHUNK_SIZE;
    auto csrOffset = AdjListHeaders::getCSROffset(header);
    auto pos = AdjListHeaders::getAdjListLen(header) - reversePos;
    cursor.idx = metadata.chunksPagesMap[chunkId][(csrOffset + pos) / numElementsInAPage];
    cursor.offset = numBytesPerElement * ((csrOffset + pos) % numElementsInAPage);
}

void AdjAndPropertyListsLoaderHelper::calculateAdjListHeadersForIndexTask(Direction dir,
    label_t nodeLabel, node_offset_t numNodeOffsets, uint32_t numPerPage, listSizes_t* listSizes,
    AdjListHeaders* adjListHeaders, shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: adjListHeaders={0:p}", (void*)adjListHeaders);
    auto numChunks = numNodeOffsets / BaseLists::LISTS_CHUNK_SIZE;
    if (0 != numNodeOffsets % BaseLists::LISTS_CHUNK_SIZE) {
        numChunks++;
    }
    node_offset_t nodeOffset = 0u;
    uint64_t lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto csrOffset = 0u;
        auto numElementsInChunk = min(nodeOffset + BaseLists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < numElementsInChunk; i++) {
            auto relCount = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            uint32_t header;
            if (relCount >= numPerPage) {
                header = 0x80000000 + (lAdjListsIdx++ & 0x7fffffff);
            } else {
                header = ((csrOffset & 0xfffff) << 11) + (relCount & 0x7ff);
                csrOffset += relCount;
            }
            (*adjListHeaders).headers[nodeOffset] = header;
            nodeOffset++;
        }
    }
    logger->trace("End: adjListHeaders={0:p}", (void*)adjListHeaders);
}

void AdjAndPropertyListsLoaderHelper::calculateListsMetadataForListsTask(uint64_t numNodeOffsets,
    uint32_t numPerPage, listSizes_t* listSizes, AdjListHeaders* adjListHeaders,
    ListsMetadata* listsMetadata, shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: listsMetadata={0:p} adjListHeaders={1:p}", (void*)listsMetadata,
        (void*)adjListHeaders);
    auto globalPageId = 0u;
    auto numChunks = numNodeOffsets / BaseLists::LISTS_CHUNK_SIZE;
    if (0 != numNodeOffsets % BaseLists::LISTS_CHUNK_SIZE) {
        numChunks++;
    }
    (*listsMetadata).chunksPagesMap.resize(numChunks);
    node_offset_t nodeOffset = 0u;
    auto lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto pageId = 0u, csrOffsetInPage = 0u;
        auto numElementsInChunk = min(nodeOffset + BaseLists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < numElementsInChunk; i++) {
            auto relCount = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            if (AdjListHeaders::isALargeAdjList(adjListHeaders->headers[nodeOffset])) {
                (*listsMetadata).largeListsPagesMap.resize(lAdjListsIdx + 1);
                auto numPages = relCount / numPerPage;
                if (0 != relCount % numPerPage) {
                    numPages++;
                }
                (*listsMetadata).largeListsPagesMap[lAdjListsIdx].resize(numPages + 1);
                (*listsMetadata).largeListsPagesMap[lAdjListsIdx][0] = relCount;
                for (auto i = 1u; i <= numPages; i++) {
                    (*listsMetadata).largeListsPagesMap[lAdjListsIdx][i] = globalPageId++;
                }
                lAdjListsIdx++;
            } else {
                while (relCount + csrOffsetInPage > numPerPage) {
                    relCount -= (numPerPage - csrOffsetInPage);
                    pageId++;
                    csrOffsetInPage = 0;
                }
                csrOffsetInPage += relCount;
            }
            nodeOffset++;
        }
        if (pageId != 0 || csrOffsetInPage != 0) {
            (*listsMetadata).chunksPagesMap[chunkId].resize(pageId + 1);
            for (auto i = 0u; i < pageId + 1; i++) {
                (*listsMetadata).chunksPagesMap[chunkId][i] = globalPageId++;
            }
        }
    }
    (*listsMetadata).numPages = globalPageId;
    logger->trace("End: listsMetadata={0:p} adjListHeaders={1:p}", (void*)listsMetadata,
        (void*)adjListHeaders);
}

void AdjAndPropertyListsLoaderHelper::sortOverflowStringsOfPropertyListsTask(
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemPropertyPages* propertyLists,
    AdjListHeaders* adjListsHeaders, ListsMetadata* listsMetadata,
    InMemStringOverflowPages* unorderedStringOverflow,
    InMemStringOverflowPages* orderedStringOverflow, shared_ptr<spdlog::logger> logger) {
    PageCursor unorderedStringOverflowCursor, orderedStringOverflowCursor, propertyListCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        auto header = adjListsHeaders->headers[offsetStart];
        uint32_t len = 0;
        if (AdjListHeaders::isALargeAdjList(header)) {
            len = listsMetadata->largeListsPagesMap[header & 0x7fffffff][0];
        } else {
            len = AdjListHeaders::getAdjListLen(header);
        }
        for (auto pos = len; pos > 0; pos--) {
            calculatePageCursor(header, pos, getDataTypeSize(STRING), offsetStart,
                propertyListCursor, *listsMetadata);
            auto valPtr = reinterpret_cast<gf_string_t*>(propertyLists->get(propertyListCursor));
            if (12 < valPtr->len && 0xffffffff != valPtr->len) {
                unorderedStringOverflowCursor.idx = 0;
                valPtr->setOverflowPtrToPageCursor(unorderedStringOverflowCursor);
                orderedStringOverflow->copyOverflowString(orderedStringOverflowCursor,
                    unorderedStringOverflow->get(unorderedStringOverflowCursor), valPtr);
            }
        }
    }
}

} // namespace loader
} // namespace graphflow
