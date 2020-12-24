#include "src/loader/include/adj_and_property_columns_loader_helper.h"

#include "src/storage/include/stores/rels_store.h"

namespace graphflow {
namespace loader {

AdjAndPropertyColumnsLoaderHelper::AdjAndPropertyColumnsLoaderHelper(
    RelLabelDescription& description, ThreadPool& threadPool, const Graph& graph,
    const Catalog& catalog, const string outputDirectory, shared_ptr<spdlog::logger> logger)
    : logger{logger}, description{description},
      threadPool{threadPool}, graph{graph}, catalog{catalog}, outputDirectory{outputDirectory} {
    if (description.hasProperties()) {
        if (description.isSingleCardinalityPerDir[FWD]) {
            buildInMemPropertyColumns(FWD);
        } else if (description.isSingleCardinalityPerDir[BWD]) {
            buildInMemPropertyColumns(BWD);
        }
    }
    buildInMemAdjColumns();
}

void AdjAndPropertyColumnsLoaderHelper::setRel(
    const Direction& dir, const vector<nodeID_t>& nodeIDs) {
    PageCursor cursor;
    calculatePageCursor(description.getRelSize(dir), nodeIDs[dir].offset, cursor);
    dirLabelAdjColumns[dir][nodeIDs[dir].label]->set(cursor, nodeIDs[!dir]);
}

void AdjAndPropertyColumnsLoaderHelper::setProperty(
    const nodeID_t& nodeID, const uint32_t& propertyIdx, const uint8_t* val, const DataType& type) {
    PageCursor cursor;
    calculatePageCursor(getDataTypeSize(type), nodeID.offset, cursor);
    labelPropertyIdxPropertyColumn[nodeID.label][propertyIdx]->set(cursor, val);
}

void AdjAndPropertyColumnsLoaderHelper::setStringProperty(const nodeID_t& nodeID,
    const uint32_t& propertyIdx, const char* originalString, PageCursor& cursor) {
    PageCursor propertyListCursor;

    calculatePageCursor(getDataTypeSize(STRING), nodeID.offset, propertyListCursor);
    gf_string_t* encodedString = reinterpret_cast<gf_string_t*>(
        labelPropertyIdxPropertyColumn[nodeID.label][propertyIdx]->get(propertyListCursor));
    (*labelPropertyIdxStringOverflowPages)[nodeID.label][propertyIdx]->set(
        originalString, cursor, encodedString);
}

void AdjAndPropertyColumnsLoaderHelper::sortOverflowStrings() {
    logger->info("Ordering String Rel Property Columns...");
    auto orderedStringOverflows =
        make_unique<labelPropertyIdxStringOverflow_t>(catalog.getNodeLabelsCount());
    auto dir = description.isSingleCardinalityPerDir[FWD] ? FWD : BWD;
    for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
        (*orderedStringOverflows)[nodeLabel].resize(description.propertyMap->size());
        for (auto property = description.propertyMap->begin();
             property != description.propertyMap->end(); property++) {
            if (STRING == property->second.dataType) {
                auto fname = RelsStore::getRelPropertyColumnFname(
                    outputDirectory, description.label, nodeLabel, property->first);
                (*orderedStringOverflows)[nodeLabel][property->second.idx] =
                    make_unique<InMemStringOverflowPages>(fname + ".ovf");
                auto numNodes = graph.getNumNodesPerLabel()[nodeLabel];
                auto numBuckets = numNodes / 256;
                if (0 != numNodes / 256) {
                    numBuckets++;
                }
                node_offset_t offsetStart = 0, offsetEnd = 0;
                for (auto i = 0u; i < numBuckets; i++) {
                    offsetStart = offsetEnd;
                    offsetEnd = min(offsetStart + 256, numNodes);
                    threadPool.execute(sortOverflowStringsofPropertyColumnTask, offsetStart,
                        offsetEnd,
                        labelPropertyIdxPropertyColumn[nodeLabel][property->second.idx].get(),
                        (*labelPropertyIdxStringOverflowPages)[nodeLabel][property->second.idx]
                            .get(),
                        (*orderedStringOverflows)[nodeLabel][property->second.idx].get(), logger);
                }
            }
        }
    }
    threadPool.wait();
    labelPropertyIdxStringOverflowPages = move(orderedStringOverflows);
}

void AdjAndPropertyColumnsLoaderHelper::saveToFile() {
    logger->info("Writing AdjColumns and Rel Property Columns to disk...");
    for (auto& dir : DIRS) {
        if (description.isSingleCardinalityPerDir[dir]) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                threadPool.execute([&](InMemAdjPages* x) { x->saveToFile(); },
                    dirLabelAdjColumns[dir][nodeLabel].get());
            }
        }
    }
    if (description.hasProperties() && !description.requirePropertyLists()) {
        auto dir = description.isSingleCardinalityPerDir[FWD] ? FWD : BWD;
        for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
            for (auto property = description.propertyMap->begin();
                 property != description.propertyMap->end(); property++) {
                threadPool.execute([&](InMemPropertyPages* x) { x->saveToFile(); },
                    reinterpret_cast<InMemPropertyPages*>(
                        labelPropertyIdxPropertyColumn[nodeLabel][property->second.idx].get()));
                if (STRING == property->second.dataType) {
                    threadPool.execute([&](InMemStringOverflowPages* x) { x->saveToFile(); },
                        (*labelPropertyIdxStringOverflowPages)[nodeLabel][property->second.idx]
                            .get());
                }
            }
        }
    }
    threadPool.wait();
}

void AdjAndPropertyColumnsLoaderHelper::buildInMemPropertyColumns(Direction dir) {
    logger->info("Creating InMemProperty Columns...");
    labelPropertyIdxPropertyColumn.resize(catalog.getNodeLabelsCount());
    labelPropertyIdxStringOverflowPages =
        make_unique<labelPropertyIdxStringOverflow_t>(catalog.getNodeLabelsCount());
    for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
        labelPropertyIdxPropertyColumn[nodeLabel].resize((*description.propertyMap).size());
        (*labelPropertyIdxStringOverflowPages)[nodeLabel].resize((*description.propertyMap).size());
        auto numElements = graph.getNumNodesPerLabel()[nodeLabel];
        for (auto property = description.propertyMap->begin();
             property != description.propertyMap->end(); property++) {
            auto fname = RelsStore::getRelPropertyColumnFname(
                outputDirectory, description.label, nodeLabel, property->first);
            uint32_t numElementsPerPage = PAGE_SIZE / getDataTypeSize(property->second.dataType);
            uint64_t numPages = numElements / numElementsPerPage;
            if (0 != numElements % numElementsPerPage) {
                numPages++;
            }
            labelPropertyIdxPropertyColumn[nodeLabel][property->second.idx] =
                make_unique<InMemPropertyPages>(
                    fname, numPages, getDataTypeSize(property->second.dataType));
            if (STRING == property->second.dataType) {
                (*labelPropertyIdxStringOverflowPages)[nodeLabel][property->second.idx] =
                    make_unique<InMemStringOverflowPages>(fname + ".ovf");
            }
        }
    }
}

void AdjAndPropertyColumnsLoaderHelper::buildInMemAdjColumns() {
    logger->info("Creating InMemAdjColumns...");
    for (auto& dir : DIRS) {
        if (description.isSingleCardinalityPerDir[dir]) {
            dirLabelAdjColumns[dir].resize(catalog.getNodeLabelsCount());
            for (auto boundNodeLabel : description.nodeLabelsPerDir[dir]) {
                auto fname = RelsStore::getAdjColumnIndexFname(
                    outputDirectory, description.label, boundNodeLabel, dir);
                uint32_t numElementsPerPage = PAGE_SIZE / description.getRelSize(dir);
                auto numNodes = graph.getNumNodesPerLabel()[boundNodeLabel];
                uint64_t numPages = numNodes / numElementsPerPage;
                if (0 != numNodes % numElementsPerPage) {
                    numPages++;
                }
                dirLabelAdjColumns[dir][boundNodeLabel] = make_unique<InMemAdjPages>(fname,
                    numPages, description.numBytesSchemePerDir[dir].first,
                    description.numBytesSchemePerDir[dir].second);
            }
        }
    }
}

void AdjAndPropertyColumnsLoaderHelper::calculatePageCursor(
    const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset, PageCursor& cursor) {
    auto numElementsPerPage = PAGE_SIZE / numBytesPerElement;
    cursor.idx = nodeOffset / numElementsPerPage;
    cursor.offset = numBytesPerElement * (nodeOffset % numElementsPerPage);
}

void AdjAndPropertyColumnsLoaderHelper::sortOverflowStringsofPropertyColumnTask(
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemPropertyPages* propertyColumn,
    InMemStringOverflowPages* unorderedStringOverflow,
    InMemStringOverflowPages* orderedStringOverflow, shared_ptr<spdlog::logger> logger) {
    PageCursor unorderedStringOverflowCursor, orderedStringOverflowCursor, propertyListCursor;
    unorderedStringOverflowCursor.idx = 0;
    unorderedStringOverflowCursor.offset = 0;
    for (; offsetStart < offsetEnd; offsetStart++) {
        calculatePageCursor(getDataTypeSize(STRING), offsetStart, propertyListCursor);
        auto valPtr = reinterpret_cast<gf_string_t*>(propertyColumn->get(propertyListCursor));
        auto len = ((uint32_t*)valPtr)[0];
        if (len > 12 && 0xffffffff != len) {
            valPtr->setOverflowPtrToPageCursor(unorderedStringOverflowCursor);
            orderedStringOverflow->copyOverflowString(orderedStringOverflowCursor,
                unorderedStringOverflow->get(unorderedStringOverflowCursor), valPtr);
        }
    }
}

} // namespace loader
} // namespace graphflow
