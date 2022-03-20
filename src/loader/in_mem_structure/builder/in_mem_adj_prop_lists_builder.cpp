#include "src/loader/include/in_mem_structure/builder/in_mem_adj_prop_lists_builder.h"

#include "spdlog/sinks/stdout_sinks.h"

#include "src/common/include/type_utils.h"

namespace graphflow {
namespace loader {

InMemAdjAndPropertyListsBuilder::InMemAdjAndPropertyListsBuilder(RelLabelDescription& description,
    TaskScheduler& taskScheduler, const Graph& graph, const string& outputDirectory)
    : InMemStructuresBuilderForRels(description, taskScheduler, graph, outputDirectory) {
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

void InMemAdjAndPropertyListsBuilder::buildAdjListsHeadersAndListsMetadata(
    LoaderProgressBar* progressBar) {
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
    initAdjListHeaders(progressBar);
    initAdjListsAndPropertyListsMetadata(progressBar);
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
        calcPageElementCursor(header, pos[direction], Types::getDataTypeSize(type),
            nodeIDs[direction].offset, cursor,
            directionLabelPropertyIdxPropertyListsMetadata[direction][nodeIDs[direction].label]
                                                          [propertyIdx],
            true /*hasNULLBytes*/);
        directionLabelPropertyIdxPropertyLists[direction][nodeIDs[direction].label][propertyIdx]
            ->set(cursor, val);
    }
}

void InMemAdjAndPropertyListsBuilder::setStringProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, uint32_t propertyIdx, const char* strVal,
    PageByteCursor& stringOverflowCursor) {
    auto gfString = (*propertyIdxUnordStringOverflowPages)[propertyIdx]->addString(
        strVal, stringOverflowCursor);
    setProperty(pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&gfString), STRING);
}

void InMemAdjAndPropertyListsBuilder::setListProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, uint32_t propertyIdx, const Literal& listVal,
    PageByteCursor& listOverflowCursor) {
    auto gfList =
        (*propertyIdxUnordStringOverflowPages)[propertyIdx]->addList(listVal, listOverflowCursor);
    setProperty(pos, nodeIDs, propertyIdx, reinterpret_cast<uint8_t*>(&gfList), LIST);
}

// todo(Guodong): Should we sort on list, too?
void InMemAdjAndPropertyListsBuilder::sortOverflowStrings(LoaderProgressBar* progressBar) {
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
                        make_unique<InMemOverflowPages>(
                            OverflowPages::getOverflowPagesFName(fName));
                    auto numNodes = graph.getNumNodesPerLabel()[nodeLabel];
                    auto numBuckets = numNodes / 256;
                    if (0 != numNodes % 256) {
                        numBuckets++;
                    }
                    node_offset_t offsetStart = 0, offsetEnd = 0;
                    auto idx = property.id;
                    progressBar->addAndStartNewJob(
                        "Sorting overflow string buckets for property: " +
                            graph.getCatalog().getRelLabelName(description.label) + "." +
                            property.name,
                        numBuckets);
                    for (auto bucketIdx = 0u; bucketIdx < numBuckets; bucketIdx++) {
                        offsetStart = offsetEnd;
                        offsetEnd = min(offsetStart + 256, numNodes);
                        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                            sortOverflowStringsOfPropertyListsTask, offsetStart, offsetEnd,
                            directionLabelPropertyIdxPropertyLists[direction][nodeLabel][idx].get(),
                            &directionLabelAdjListHeaders[direction][nodeLabel],
                            &directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel]
                                                                           [idx],
                            (*propertyIdxUnordStringOverflowPages)[idx].get(),
                            (*directionLabelPropertyIdxStringOverflowPages)[direction][nodeLabel]
                                                                           [idx]
                                                                               .get(),
                            progressBar));
                    }
                    taskScheduler.waitAllTasksToCompleteOrError();
                }
            }
        }
    }
    propertyIdxUnordStringOverflowPages.reset();
    logger->debug("Done ordering String Rel PropertyList.");
}

void InMemAdjAndPropertyListsBuilder::saveToFile(LoaderProgressBar* progressBar) {
    logger->debug("Writing AdjLists and Rel Property Lists to disk.");
    progressBar->addAndStartNewJob("Writing adjacency lists to disk for relationship: " +
                                       graph.getCatalog().getRelLabelName(description.label),
        1);
    for (auto& direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                taskScheduler.scheduleTask(
                    LoaderTaskFactory::createLoaderTask([&](InMemAdjPages* x) { x->saveToFile(); },
                        directionLabelAdjLists[direction][nodeLabel].get()));
                auto fName = RelsStore::getAdjListsFName(
                    outputDirectory, description.label, nodeLabel, direction);
                taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                    [&](ListsMetadata* x, const string& fName) { x->saveToDisk(fName); },
                    &directionLabelAdjListsMetadata[direction][nodeLabel], fName));
                taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                    [&](ListHeaders* x, const string& fName) { x->saveToDisk(fName); },
                    &directionLabelAdjListHeaders[direction][nodeLabel], fName));
            }
        }
    }
    if (description.requirePropertyLists()) {
        for (auto& direction : DIRECTIONS) {
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                for (auto& property : description.properties) {
                    auto idx = property.id;
                    if (directionLabelPropertyIdxPropertyLists[direction][nodeLabel][idx]) {
                        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                            [&](InMemPropertyPages* x) { x->saveToFile(); },
                            directionLabelPropertyIdxPropertyLists[direction][nodeLabel][idx]
                                .get()));
                        if (STRING == property.dataType || LIST == property.dataType) {
                            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                                [&](InMemOverflowPages* x) { x->saveToFile(); },
                                (*directionLabelPropertyIdxStringOverflowPages)[direction]
                                                                               [nodeLabel][idx]
                                                                                   .get()));
                        }
                        auto fName = RelsStore::getRelPropertyListsFName(outputDirectory,
                            description.label, nodeLabel, direction, property.name);
                        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                            [&](ListsMetadata* x, const string& fName) { x->saveToDisk(fName); },
                            &directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel]
                                                                           [idx],
                            fName));
                    }
                }
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    progressBar->incrementTaskFinished();
    logger->debug("Done writing AdjLists and Rel Property Lists to disk.");
}

void InMemAdjAndPropertyListsBuilder::initAdjListHeaders(LoaderProgressBar* progressBar) {
    logger->debug("Initializing AdjListHeaders.");
    progressBar->addAndStartNewJob("Initializing adjacency lists headers for relationship: " +
                                       graph.getCatalog().getRelLabelName(description.label),
        1);
    for (auto direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            auto relSize =
                description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes();
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                    calculateListHeadersTask, graph.getNumNodesPerLabel()[nodeLabel], relSize,
                    directionLabelListSizes[direction][nodeLabel].get(),
                    &directionLabelAdjListHeaders[direction][nodeLabel], logger));
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    progressBar->incrementTaskFinished();
    logger->debug("Done initializing AdjListHeaders.");
}

void InMemAdjAndPropertyListsBuilder::initAdjListsAndPropertyListsMetadata(
    LoaderProgressBar* progressBar) {
    logger->debug("Initializing AdjLists and PropertyLists Metadata.");
    progressBar->addAndStartNewJob(
        "Initializing adjacency and property lists metadata for relationship: " +
            graph.getCatalog().getRelLabelName(description.label),
        1);
    for (auto direction : DIRECTIONS) {
        if (!description.isSingleMultiplicityPerDirection[direction]) {
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                    calculateListsMetadataTask, graph.getNumNodesPerLabel()[nodeLabel],
                    description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes(),
                    directionLabelListSizes[direction][nodeLabel].get(),
                    &directionLabelAdjListHeaders[direction][nodeLabel],
                    &directionLabelAdjListsMetadata[direction][nodeLabel], false /*hasNULLBytes*/,
                    logger));
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
                    taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                        calculateListsMetadataTask, numNodeOffsets,
                        Types::getDataTypeSize(property.dataType), listsSizes,
                        &directionLabelAdjListHeaders[direction][nodeLabel],
                        &directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel][idx],
                        true /*hasNULLBytes*/, logger));
                }
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    progressBar->incrementTaskFinished();
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
                        Types::getDataTypeSize(property.dataType),
                        directionLabelPropertyIdxPropertyListsMetadata[direction][nodeLabel][idx]
                            .numPages);
            }
        }
    }
    propertyIdxUnordStringOverflowPages =
        make_unique<vector<unique_ptr<InMemOverflowPages>>>(description.properties.size());
    for (auto& property : description.properties) {
        if (STRING == property.dataType || LIST == property.dataType) {
            (*propertyIdxUnordStringOverflowPages)[property.id] = make_unique<InMemOverflowPages>();
        }
    }
    logger->debug("Done creating InMemPropertyLists.");
}

void InMemAdjAndPropertyListsBuilder::sortOverflowStringsOfPropertyListsTask(
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemPropertyPages* propertyLists,
    ListHeaders* adjListsHeaders, ListsMetadata* listsMetadata,
    InMemOverflowPages* unorderedStringOverflowPages, InMemOverflowPages* orderedStringOverflow,
    LoaderProgressBar* progressBar) {
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
            calcPageElementCursor(header, pos, Types::getDataTypeSize(STRING), offsetStart,
                propertyListCursor, *listsMetadata, true /*hasNULLBytes*/);
            auto valPtr =
                reinterpret_cast<gf_string_t*>(propertyLists->getPtrToMemLoc(propertyListCursor));
            if (gf_string_t::SHORT_STR_LENGTH < valPtr->len && 0xffffffff != valPtr->len) {
                unorderedStringOverflowCursor.idx = 0;
                TypeUtils::decodeOverflowPtr(valPtr->overflowPtr, unorderedStringOverflowCursor.idx,
                    unorderedStringOverflowCursor.offset);
                orderedStringOverflow->copyOverflowString(orderedStringOverflowCursor,
                    unorderedStringOverflowPages->getPtrToMemLoc(unorderedStringOverflowCursor),
                    valPtr);
            }
        }
    }
    progressBar->incrementTaskFinished();
}

} // namespace loader
} // namespace graphflow
