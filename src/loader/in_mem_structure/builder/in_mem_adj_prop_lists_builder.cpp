#include "src/loader/include/in_mem_structure/builder/in_mem_adj_prop_lists_builder.h"

#include "spdlog/sinks/stdout_sinks.h"

namespace graphflow {
namespace loader {

InMemAdjAndPropertyListsBuilder::InMemAdjAndPropertyListsBuilder(RelLabelDescription& description,
    ThreadPool& threadPool, const Graph& graph, const string& outputDirectory)
    : InMemStructuresBuilderForRels(description, threadPool, graph, outputDirectory) {
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

void InMemAdjAndPropertyListsBuilder::buildAdjListsHeadersAndListsMetadata() {
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

void InMemAdjAndPropertyListsBuilder::buildInMemStructures() {
    if (description.requirePropertyLists()) {
        buildInMemPropertyLists();
    }
    buildInMemAdjLists();
}

void InMemAdjAndPropertyListsBuilder::setRel(
    uint64_t pos, Direction direction, const vector<nodeID_t>& nodeIDs) {
    PageElementCursor cursor;
    auto header = directionLabelAdjListHeaders[direction][nodeIDs[direction].label]
                      .headers[nodeIDs[direction].offset];
    calcPageElementCursor(header, pos,
        description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes(),
        nodeIDs[direction].offset, cursor,
        directionLabelAdjListsMetadata[direction][nodeIDs[direction].label],
        false /*hasNULLBytes*/);
    directionLabelAdjLists[direction][nodeIDs[direction].label]->set(cursor, nodeIDs[!direction]);
}

void InMemAdjAndPropertyListsBuilder::setProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, uint32_t propertyIdx, const uint8_t* val, DataType type) {
    PageElementCursor cursor;
    for (auto& direction : DIRECTIONS) {
        auto header = directionLabelAdjListHeaders[direction][nodeIDs[direction].label]
                          .headers[nodeIDs[direction].offset];
        calcPageElementCursor(header, pos[direction], TypeUtils::getDataTypeSize(type),
            nodeIDs[direction].offset, cursor,
            directionLabelPropertyIdxPropertyListsMetadata[direction][nodeIDs[direction].label]
                                                          [propertyIdx],
            true /*hasNULLBytes*/);
        directionLabelPropertyIdxPropertyLists[direction][nodeIDs[direction].label][propertyIdx]
            ->set(cursor, val);
    }
}

void InMemAdjAndPropertyListsBuilder::setStringProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, const uint32_t& propertyIdx, const char* strVal,
    PageByteCursor& stringOverflowCursor) {
    gf_string_t gfString;
    (*propertyIdxUnordStringOverflowPages)[propertyIdx]->setStrInOvfPageAndPtrInEncString(
        strVal, stringOverflowCursor, &gfString);
    setProperty(pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&gfString), STRING);
}

void InMemAdjAndPropertyListsBuilder::sortOverflowStrings() {
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
                        make_unique<InMemStringOverflowPages>(
                            StringOverflowPages::getStringOverflowPagesFName(fName));
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

void InMemAdjAndPropertyListsBuilder::saveToFile() {
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

void InMemAdjAndPropertyListsBuilder::initAdjListHeaders() {
    logger->debug("Initializing AdjListHeaders.");
    for (auto direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            auto relSize =
                description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes();
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                threadPool.execute(calculateListHeadersTask, graph.getNumNodesPerLabel()[nodeLabel],
                    relSize, directionLabelListSizes[direction][nodeLabel].get(),
                    &directionLabelAdjListHeaders[direction][nodeLabel], logger);
            }
        }
    }
    threadPool.wait();
    logger->debug("Done initializing AdjListHeaders.");
}

void InMemAdjAndPropertyListsBuilder::initAdjListsAndPropertyListsMetadata() {
    logger->debug("Initializing AdjLists and PropertyLists Metadata.");
    for (auto direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                threadPool.execute(calculateListsMetadataTask,
                    graph.getNumNodesPerLabel()[nodeLabel],
                    description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes(),
                    directionLabelListSizes[direction][nodeLabel].get(),
                    &directionLabelAdjListHeaders[direction][nodeLabel],
                    &directionLabelAdjListsMetadata[direction][nodeLabel], false /*hasNULLBytes*/,
                    logger);
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
                    threadPool.execute(calculateListsMetadataTask, numNodeOffsets,
                        TypeUtils::getDataTypeSize(property.dataType), listsSizes,
                        &directionLabelAdjListHeaders[direction][nodeLabel],
                        &directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel][idx],
                        true /*hasNULLBytes*/, logger);
                }
            }
        }
    }
    threadPool.wait();
    logger->debug("Done initializing AdjLists and PropertyLists Metadata.");
}

void InMemAdjAndPropertyListsBuilder::buildInMemAdjLists() {
    logger->debug("Creating InMemAdjLists.");
    for (auto& direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            directionLabelAdjLists[direction].resize(graph.getCatalog().getNodeLabelsCount());
            for (auto boundNodeLabel : description.nodeLabelsPerDirection[direction]) {
                auto fName = RelsStore::getAdjListsFName(
                    outputDirectory, description.label, boundNodeLabel, direction);
                directionLabelAdjLists[direction][boundNodeLabel] =
                    make_unique<InMemAdjPages>(fName,
                        description.nodeIDCompressionSchemePerDirection[direction]
                            .getNumBytesForLabel(),
                        description.nodeIDCompressionSchemePerDirection[direction]
                            .getNumBytesForOffset(),
                        directionLabelAdjListsMetadata[direction][boundNodeLabel].numPages,
                        false /*hasNULLBytes*/);
            }
        }
    }
    logger->debug("Done creating InMemAdjLists.");
}

void InMemAdjAndPropertyListsBuilder::buildInMemPropertyLists() {
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
                        TypeUtils::getDataTypeSize(property.dataType),
                        directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel][idx]
                            .numPages);
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

void InMemAdjAndPropertyListsBuilder::sortOverflowStringsOfPropertyListsTask(
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemPropertyPages* propertyLists,
    ListHeaders* adjListsHeaders, ListsMetadata* listsMetadata,
    InMemStringOverflowPages* unorderedStringOverflowPages,
    InMemStringOverflowPages* orderedStringOverflow) {
    PageByteCursor unorderedStringOverflowCursor, orderedStringOverflowCursor;
    PageElementCursor propertyListCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        auto header = adjListsHeaders->headers[offsetStart];
        uint32_t len = 0;
        if (ListHeaders::isALargeList(header)) {
            len = listsMetadata->getNumElementsInLargeLists(header & 0x7fffffff);
        } else {
            len = ListHeaders::getSmallListLen(header);
        }
        for (auto pos = len; pos > 0; pos--) {
            calcPageElementCursor(header, pos, TypeUtils::getDataTypeSize(STRING), offsetStart,
                propertyListCursor, *listsMetadata, true /*hasNULLBytes*/);
            auto valPtr =
                reinterpret_cast<gf_string_t*>(propertyLists->getPtrToMemLoc(propertyListCursor));
            if (gf_string_t::SHORT_STR_LENGTH < valPtr->len && 0xffffffff != valPtr->len) {
                unorderedStringOverflowCursor.idx = 0;
                valPtr->getOverflowPtrInfo(
                    unorderedStringOverflowCursor.idx, unorderedStringOverflowCursor.offset);
                orderedStringOverflow->copyOverflowString(orderedStringOverflowCursor,
                    unorderedStringOverflowPages->getPtrToMemLoc(unorderedStringOverflowCursor),
                    valPtr);
            }
        }
    }
}

} // namespace loader
} // namespace graphflow
