#include "src/loader/include/adj_and_prop_lists_builder.h"

#include "spdlog/sinks/stdout_sinks.h"

namespace graphflow {
namespace loader {

AdjAndPropertyListsBuilder::AdjAndPropertyListsBuilder(RelLabelDescription& description,
    ThreadPool& threadPool, const Graph& graph, const string& outputDirectory)
    : AdjAndPropertyStructuresBuilder(description, threadPool, graph, outputDirectory) {
    for (auto& direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            directionLabelListSizes[direction].resize(graph.getCatalog().getNodeLabelsCount());
            directionLabelNumRels[direction] =
                make_unique<listSizes_t>(graph.getCatalog().getNodeLabelsCount());
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                directionLabelListSizes[direction][nodeLabel] =
                    make_unique<listSizes_t>(graph.getNumNodesPerLabel()[nodeLabel]);
            }
        }
    }
}

void AdjAndPropertyListsBuilder::buildAdjListsHeadersAndListsMetadata() {
    for (auto& direction : DIRECTIONS) {
        directionLabelAdjListHeaders[direction].resize(graph.getCatalog().getNodeLabelsCount());
        for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
            directionLabelAdjListHeaders[direction][nodeLabel].init(
                graph.getNumNodesPerLabel()[nodeLabel]);
        }
        directionLabelAdjListsMetadata[direction].resize(graph.getCatalog().getNodeLabelsCount());
    }
    if (description.requirePropertyLists()) {
        for (auto& direction : DIRECTIONS) {
            directionLabelPropertyIdxPropertyListsMetadata[direction].resize(
                graph.getCatalog().getNodeLabelsCount());
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel].resize(
                    description.properties.size());
            }
        }
    }
    initAdjListHeaders();
    initAdjListsAndPropertyListsMetadata();
}

void AdjAndPropertyListsBuilder::buildInMemStructures() {
    if (description.requirePropertyLists()) {
        buildInMemPropertyLists();
    }
    buildInMemAdjLists();
}

void AdjAndPropertyListsBuilder::setRel(
    const uint64_t& pos, const Direction& direction, const vector<nodeID_t>& nodeIDs) {
    PageCursor cursor;
    auto header = directionLabelAdjListHeaders[direction][nodeIDs[direction].label]
                      .headers[nodeIDs[direction].offset];
    ListsLoaderHelper::calculatePageCursor(header, pos,
        description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes(),
        nodeIDs[direction].offset, cursor,
        directionLabelAdjListsMetadata[direction][nodeIDs[direction].label]);
    directionLabelAdjLists[direction][nodeIDs[direction].label]->setNbrNode(
        cursor, nodeIDs[!direction]);
}

void AdjAndPropertyListsBuilder::setProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, const uint32_t& propertyIdx, const uint8_t* val,
    const DataType& type) {
    PageCursor cursor;
    for (auto& direction : DIRECTIONS) {
        auto header = directionLabelAdjListHeaders[direction][nodeIDs[direction].label]
                          .headers[nodeIDs[direction].offset];
        ListsLoaderHelper::calculatePageCursor(header, pos[direction],
            TypeUtils::getDataTypeSize(type), nodeIDs[direction].offset, cursor,
            directionLabelPropertyIdxPropertyListsMetadata[direction][nodeIDs[direction].label]
                                                          [propertyIdx]);
        directionLabelPropertyIdxPropertyLists[direction][nodeIDs[direction].label][propertyIdx]
            ->setPorperty(cursor, val);
    }
}

void AdjAndPropertyListsBuilder::setStringProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, const uint32_t& propertyIdx, const char* strVal,
    PageCursor& stringOverflowCursor) {
    PageCursor propertyListCursor;
    ListsLoaderHelper::calculatePageCursor(
        directionLabelAdjListHeaders[FWD][nodeIDs[FWD].label].headers[nodeIDs[FWD].offset],
        pos[FWD], TypeUtils::getDataTypeSize(STRING), nodeIDs[FWD].offset, propertyListCursor,
        directionLabelPropertyIdxPropertyListsMetadata[FWD][nodeIDs[FWD].label][propertyIdx]);
    auto encodedStrFwd = reinterpret_cast<gf_string_t*>(
        directionLabelPropertyIdxPropertyLists[FWD][nodeIDs[FWD].label][propertyIdx]
            ->getPtrToMemLoc(propertyListCursor));
    (*propertyIdxUnordStringOverflowPages)[propertyIdx]->setStrInOvfPageAndPtrInEncString(
        strVal, stringOverflowCursor, encodedStrFwd);
    ListsLoaderHelper::calculatePageCursor(
        directionLabelAdjListHeaders[BWD][nodeIDs[BWD].label].headers[nodeIDs[BWD].offset],
        pos[BWD], TypeUtils::getDataTypeSize(STRING), nodeIDs[BWD].offset, propertyListCursor,
        directionLabelPropertyIdxPropertyListsMetadata[BWD][nodeIDs[BWD].label][propertyIdx]);
    auto encodedStrBwd = reinterpret_cast<gf_string_t*>(
        directionLabelPropertyIdxPropertyLists[BWD][nodeIDs[BWD].label][propertyIdx]
            ->getPtrToMemLoc(propertyListCursor));
    memcpy((void*)encodedStrBwd, (void*)encodedStrFwd, TypeUtils::getDataTypeSize(STRING));
}

void AdjAndPropertyListsBuilder::sortOverflowStrings() {
    logger->debug("Ordering String Rel PropertyList.");
    directionLabelPropertyIdxStringOverflowPages =
        make_unique<directionLabelPropertyIdxStringOverflowPages_t>(2);
    for (auto& direction : DIRECTIONS) {
        (*directionLabelPropertyIdxStringOverflowPages)[direction].resize(
            graph.getCatalog().getNodeLabelsCount());
        for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
            (*directionLabelPropertyIdxStringOverflowPages)[direction][nodeLabel].resize(
                description.properties.size());
            for (auto& property : description.properties) {
                if (STRING == property.dataType) {
                    auto fName = RelsStore::getRelPropertyListsFName(
                        outputDirectory, description.label, nodeLabel, direction, property.name);
                    (*directionLabelPropertyIdxStringOverflowPages)[direction][nodeLabel][property
                                                                                              .id] =
                        make_unique<InMemStringOverflowPages>(fName + OVERFLOW_FILE_SUFFIX);
                    auto numNodes = graph.getNumNodesPerLabel()[nodeLabel];
                    auto numBuckets = numNodes / 256;
                    if (0 != numNodes % 256) {
                        numBuckets++;
                    }
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    auto idx = property.id;
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        threadPool.execute(sortOverflowStringsOfPropertyListsTask, offsetStart,
                            offsetEnd,
                            directionLabelPropertyIdxPropertyLists[direction][nodeLabel][idx].get(),
                            &directionLabelAdjListHeaders[direction][nodeLabel],
                            &directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel]
                                                                           [idx],
                            (*propertyIdxUnordStringOverflowPages)[idx].get(),
                            (*directionLabelPropertyIdxStringOverflowPages)[direction][nodeLabel]
                                                                           [idx]
                                                                               .get());
                    }
                }
            }
        }
    }
    threadPool.wait();
    propertyIdxUnordStringOverflowPages.reset();
    logger->debug("Done ordering String Rel PropertyList.");
}

void AdjAndPropertyListsBuilder::saveToFile() {
    logger->debug("Writing AdjLists and Rel Property Lists to disk.");
    for (auto& direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                threadPool.execute([&](InMemAdjPages* x) { x->saveToFile(); },
                    directionLabelAdjLists[direction][nodeLabel].get());
                auto fName = RelsStore::getAdjListsFName(
                    outputDirectory, description.label, nodeLabel, direction);
                threadPool.execute(
                    [&](ListsMetadata* x, const string& fName) { x->saveToDisk(fName); },
                    &directionLabelAdjListsMetadata[direction][nodeLabel], fName);
                threadPool.execute(
                    [&](ListHeaders* x, const string& fName) { x->saveToDisk(fName); },
                    &directionLabelAdjListHeaders[direction][nodeLabel], fName);
            }
        }
    }
    if (description.requirePropertyLists()) {
        for (auto& direction : DIRECTIONS) {
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                for (auto& property : description.properties) {
                    auto idx = property.id;
                    if (directionLabelPropertyIdxPropertyLists[direction][nodeLabel][idx]) {
                        threadPool.execute([&](InMemPropertyPages* x) { x->saveToFile(); },
                            directionLabelPropertyIdxPropertyLists[direction][nodeLabel][idx]
                                .get());
                        if (STRING == property.dataType) {
                            threadPool.execute(
                                [&](InMemStringOverflowPages* x) { x->saveToFile(); },
                                (*directionLabelPropertyIdxStringOverflowPages)[direction]
                                                                               [nodeLabel][idx]
                                                                                   .get());
                        }
                        auto fName = RelsStore::getRelPropertyListsFName(outputDirectory,
                            description.label, nodeLabel, direction, property.name);
                        threadPool.execute(
                            [&](ListsMetadata* x, const string& fName) { x->saveToDisk(fName); },
                            &directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel]
                                                                           [idx],
                            fName);
                    }
                }
            }
        }
    }
    threadPool.wait();
    logger->debug("Done writing AdjLists and Rel Property Lists to disk.");
}

void AdjAndPropertyListsBuilder::initAdjListHeaders() {
    logger->debug("Initializing AdjListHeaders.");
    for (auto direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            auto relSize =
                description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes();
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                threadPool.execute(ListsLoaderHelper::calculateListHeadersTask,
                    graph.getNumNodesPerLabel()[nodeLabel], PAGE_SIZE / relSize,
                    directionLabelListSizes[direction][nodeLabel].get(),
                    &directionLabelAdjListHeaders[direction][nodeLabel], logger);
            }
        }
    }
    threadPool.wait();
    logger->debug("Done initializing AdjListHeaders.");
}

void AdjAndPropertyListsBuilder::initAdjListsAndPropertyListsMetadata() {
    logger->debug("Initializing AdjLists and PropertyLists Metadata.");
    for (auto direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            auto numPerPage =
                PAGE_SIZE /
                description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes();
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                threadPool.execute(ListsLoaderHelper::calculateListsMetadataTask,
                    graph.getNumNodesPerLabel()[nodeLabel], numPerPage,
                    directionLabelListSizes[direction][nodeLabel].get(),
                    &directionLabelAdjListHeaders[direction][nodeLabel],
                    &directionLabelAdjListsMetadata[direction][nodeLabel], logger);
            }
        }
    }
    if (description.requirePropertyLists()) {
        for (auto direction : DIRECTIONS) {
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                auto listsSizes = directionLabelListSizes[direction][nodeLabel].get();
                auto numNodeOffsets = graph.getNumNodesPerLabel()[nodeLabel];
                for (auto& property : description.properties) {
                    auto idx = property.id;
                    auto numPerPage = PAGE_SIZE / TypeUtils::getDataTypeSize(property.dataType);
                    threadPool.execute(ListsLoaderHelper::calculateListsMetadataTask,
                        numNodeOffsets, numPerPage, listsSizes,
                        &directionLabelAdjListHeaders[direction][nodeLabel],
                        &directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel][idx],
                        logger);
                }
            }
        }
    }
    threadPool.wait();
    logger->debug("Done initializing AdjLists and PropertyLists Metadata.");
}

void AdjAndPropertyListsBuilder::buildInMemAdjLists() {
    logger->debug("Creating InMemAdjLists.");
    for (auto& direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            directionLabelAdjLists[direction].resize(graph.getCatalog().getNodeLabelsCount());
            for (auto boundNodeLabel : description.nodeLabelsPerDirection[direction]) {
                auto fName = RelsStore::getAdjListsFName(
                    outputDirectory, description.label, boundNodeLabel, direction);
                directionLabelAdjLists[direction][boundNodeLabel] = make_unique<InMemAdjPages>(
                    fName, directionLabelAdjListsMetadata[direction][boundNodeLabel].numPages,
                    description.nodeIDCompressionSchemePerDirection[direction]
                        .getNumBytesForLabel(),
                    description.nodeIDCompressionSchemePerDirection[direction]
                        .getNumBytesForOffset());
            }
        }
    }
    logger->debug("Done creating InMemAdjLists.");
}

void AdjAndPropertyListsBuilder::buildInMemPropertyLists() {
    logger->debug("Creating InMemPropertyLists.");
    for (auto& direction : DIRECTIONS) {
        directionLabelPropertyIdxPropertyLists[direction].resize(
            graph.getCatalog().getNodeLabelsCount());
        for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
            directionLabelPropertyIdxPropertyLists[direction][nodeLabel].resize(
                description.properties.size());
            for (auto& property : description.properties) {
                auto idx = property.id;
                auto fName = RelsStore::getRelPropertyListsFName(
                    outputDirectory, description.label, nodeLabel, direction, property.name);
                directionLabelPropertyIdxPropertyLists[direction][nodeLabel][idx] =
                    make_unique<InMemPropertyPages>(fName,
                        directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel][idx]
                            .numPages,
                        TypeUtils::getDataTypeSize(property.dataType));
            }
        }
    }
    propertyIdxUnordStringOverflowPages =
        make_unique<vector<unique_ptr<InMemStringOverflowPages>>>(description.properties.size());
    for (auto& property : description.properties) {
        if (STRING == property.dataType) {
            (*propertyIdxUnordStringOverflowPages)[property.id] =
                make_unique<InMemStringOverflowPages>();
        }
    }
    logger->debug("Done creating InMemPropertyLists.");
}

void AdjAndPropertyListsBuilder::sortOverflowStringsOfPropertyListsTask(node_offset_t offsetStart,
    node_offset_t offsetEnd, InMemPropertyPages* propertyLists, ListHeaders* adjListsHeaders,
    ListsMetadata* listsMetadata, InMemStringOverflowPages* unorderedStringOverflow,
    InMemStringOverflowPages* orderedStringOverflow) {
    PageCursor unorderedStringOverflowCursor, orderedStringOverflowCursor, propertyListCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        auto header = adjListsHeaders->headers[offsetStart];
        uint32_t len = 0;
        if (ListHeaders::isALargeList(header)) {
            len = listsMetadata->getNumElementsInLargeLists(header & 0x7fffffff);
        } else {
            len = ListHeaders::getSmallListLen(header);
        }
        for (auto pos = len; pos > 0; pos--) {
            ListsLoaderHelper::calculatePageCursor(header, pos, TypeUtils::getDataTypeSize(STRING),
                offsetStart, propertyListCursor, *listsMetadata);
            auto valPtr =
                reinterpret_cast<gf_string_t*>(propertyLists->getPtrToMemLoc(propertyListCursor));
            if (gf_string_t::SHORT_STR_LENGTH < valPtr->len && 0xffffffff != valPtr->len) {
                unorderedStringOverflowCursor.idx = 0;
                valPtr->getOverflowPtrInfo(
                    unorderedStringOverflowCursor.idx, unorderedStringOverflowCursor.offset);
                orderedStringOverflow->copyOverflowString(orderedStringOverflowCursor,
                    unorderedStringOverflow->getPtrToMemLoc(unorderedStringOverflowCursor), valPtr);
            }
        }
    }
}

} // namespace loader
} // namespace graphflow
