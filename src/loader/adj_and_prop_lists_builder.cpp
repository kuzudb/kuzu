#include "src/loader/include/adj_and_prop_lists_builder.h"

namespace graphflow {
namespace loader {

AdjAndPropertyListsBuilder::AdjAndPropertyListsBuilder(RelLabelDescription& description,
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

void AdjAndPropertyListsBuilder::buildAdjListsHeadersAndListsMetadata() {
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

void AdjAndPropertyListsBuilder::buildInMemStructures() {
    if (description.requirePropertyLists()) {
        buildInMemPropertyLists();
    }
    buildInMemAdjLists();
}

void AdjAndPropertyListsBuilder::setRel(
    const uint64_t& pos, const Direction& dir, const vector<nodeID_t>& nodeIDs) {
    PageCursor cursor;
    auto header = dirLabelAdjListHeaders[dir][nodeIDs[dir].label].headers[nodeIDs[dir].offset];
    ListsLoaderHelper::calculatePageCursor(header, pos,
        description.nodeIDCompressionSchemePerDir[dir].getNumTotalBytes(), nodeIDs[dir].offset,
        cursor, dirLabelAdjListsMetadata[dir][nodeIDs[dir].label]);
    dirLabelAdjLists[dir][nodeIDs[dir].label]->set(cursor, nodeIDs[!dir]);
}

void AdjAndPropertyListsBuilder::setProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, const uint32_t& propertyIdx, const uint8_t* val,
    const DataType& type) {
    PageCursor cursor;
    for (auto& dir : DIRS) {
        auto header = dirLabelAdjListHeaders[dir][nodeIDs[dir].label].headers[nodeIDs[dir].offset];
        ListsLoaderHelper::calculatePageCursor(header, pos[dir], getDataTypeSize(type),
            nodeIDs[dir].offset, cursor,
            dirLabelPropertyIdxPropertyListsMetadata[dir][nodeIDs[dir].label][propertyIdx]);
        dirLabelPropertyIdxPropertyLists[dir][nodeIDs[dir].label][propertyIdx]->set(cursor, val);
    }
}

void AdjAndPropertyListsBuilder::setStringProperty(const vector<uint64_t>& pos,
    const vector<nodeID_t>& nodeIDs, const uint32_t& propertyIdx, const char* strVal,
    PageCursor& stringOverflowCursor) {
    PageCursor propertyListCursor;
    ListsLoaderHelper::calculatePageCursor(
        dirLabelAdjListHeaders[FWD][nodeIDs[FWD].label].headers[nodeIDs[FWD].offset], pos[FWD],
        getDataTypeSize(STRING), nodeIDs[FWD].offset, propertyListCursor,
        dirLabelPropertyIdxPropertyListsMetadata[FWD][nodeIDs[FWD].label][propertyIdx]);
    auto encodedStrFwd = reinterpret_cast<gf_string_t*>(
        dirLabelPropertyIdxPropertyLists[FWD][nodeIDs[FWD].label][propertyIdx]->get(
            propertyListCursor));
    (*propertyIdxUnordStringOverflowPages)[propertyIdx]->set(
        strVal, stringOverflowCursor, encodedStrFwd);
    ListsLoaderHelper::calculatePageCursor(
        dirLabelAdjListHeaders[BWD][nodeIDs[BWD].label].headers[nodeIDs[BWD].offset], pos[BWD],
        getDataTypeSize(STRING), nodeIDs[BWD].offset, propertyListCursor,
        dirLabelPropertyIdxPropertyListsMetadata[BWD][nodeIDs[BWD].label][propertyIdx]);
    auto encodedStrBwd = reinterpret_cast<gf_string_t*>(
        dirLabelPropertyIdxPropertyLists[BWD][nodeIDs[BWD].label][propertyIdx]->get(
            propertyListCursor));
    memcpy(encodedStrBwd, encodedStrFwd, getDataTypeSize(STRING));
}

void AdjAndPropertyListsBuilder::sortOverflowStrings() {
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
    logger->debug("Done ordering String Rel PropertyList.");
}

void AdjAndPropertyListsBuilder::saveToFile() {
    logger->debug("Writing AdjLists and Rel Property Lists to disk.");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                dirLabelAdjLists[dir][nodeLabel]->saveToFile();
                auto fname =
                    RelsStore::getAdjListsFname(outputDirectory, description.label, nodeLabel, dir);
                threadPool.execute([&](ListsMetadata& x, string fname) { x.saveToFile(fname); },
                    dirLabelAdjListsMetadata[dir][nodeLabel], fname);
                threadPool.execute([&](ListHeaders& x, string fname) { x.saveToFile(fname); },
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
    logger->debug("Done writing AdjLists and Rel Property Lists to disk.");
}

void AdjAndPropertyListsBuilder::initAdjListHeaders() {
    logger->debug("Initializing AdjListHeaders.");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            auto relSize = description.nodeIDCompressionSchemePerDir[dir].getNumTotalBytes();
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                threadPool.execute(ListsLoaderHelper::calculateListHeadersTask,
                    graph.getNumNodesPerLabel()[nodeLabel], PAGE_SIZE / relSize,
                    dirLabelListSizes[dir][nodeLabel].get(),
                    &dirLabelAdjListHeaders[dir][nodeLabel], logger);
            }
        }
    }
    threadPool.wait();
    logger->debug("Done initializing AdjListHeaders.");
}

void AdjAndPropertyListsBuilder::initAdjListsAndPropertyListsMetadata() {
    logger->debug("Initializing AdjLists and PropertyLists Metadata.");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            auto numPerPage =
                PAGE_SIZE / description.nodeIDCompressionSchemePerDir[dir].getNumTotalBytes();
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                threadPool.execute(ListsLoaderHelper::calculateListsMetadataTask,
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
                    threadPool.execute(ListsLoaderHelper::calculateListsMetadataTask,
                        numNodeOffsets, numPerPage, listsSizes,
                        &dirLabelAdjListHeaders[dir][nodeLabel],
                        &dirLabelPropertyIdxPropertyListsMetadata[dir][nodeLabel][idx], logger);
                }
            }
        }
    }
    threadPool.wait();
    logger->debug("Done initializing AdjLists and PropertyLists Metadata.");
}

void AdjAndPropertyListsBuilder::buildInMemAdjLists() {
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
    logger->debug("Done creating InMemAdjLists.");
}

void AdjAndPropertyListsBuilder::buildInMemPropertyLists() {
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
    logger->debug("Done creating InMemPropertyLists.");
}

void AdjAndPropertyListsBuilder::sortOverflowStringsOfPropertyListsTask(node_offset_t offsetStart,
    node_offset_t offsetEnd, InMemPropertyPages* propertyLists, ListHeaders* adjListsHeaders,
    ListsMetadata* listsMetadata, InMemStringOverflowPages* unorderedStringOverflow,
    InMemStringOverflowPages* orderedStringOverflow, shared_ptr<spdlog::logger> logger) {
    PageCursor unorderedStringOverflowCursor, orderedStringOverflowCursor, propertyListCursor;
    for (; offsetStart < offsetEnd; offsetStart++) {
        auto header = adjListsHeaders->headers[offsetStart];
        uint32_t len = 0;
        if (ListHeaders::isALargeList(header)) {
            len = listsMetadata->largeListsPagesMap[header & 0x7fffffff][0];
        } else {
            len = ListHeaders::getSmallListLen(header);
        }
        for (auto pos = len; pos > 0; pos--) {
            ListsLoaderHelper::calculatePageCursor(header, pos, getDataTypeSize(STRING),
                offsetStart, propertyListCursor, *listsMetadata);
            auto valPtr = reinterpret_cast<gf_string_t*>(propertyLists->get(propertyListCursor));
            if (12 < valPtr->len && 0xffffffff != valPtr->len) {
                unorderedStringOverflowCursor.idx = 0;
                valPtr->getOverflowPtrInfo(
                    unorderedStringOverflowCursor.idx, unorderedStringOverflowCursor.offset);
                orderedStringOverflow->copyOverflowString(orderedStringOverflowCursor,
                    unorderedStringOverflow->get(unorderedStringOverflowCursor), valPtr);
            }
        }
    }
}

} // namespace loader
} // namespace graphflow
