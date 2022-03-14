#include "src/loader/include/in_mem_structure/builder/in_mem_adj_prop_cols_builder.h"

#include "spdlog/sinks/stdout_sinks.h"

namespace graphflow {
namespace loader {

InMemAdjAndPropertyColumnsBuilder::InMemAdjAndPropertyColumnsBuilder(
    RelLabelDescription& description, TaskScheduler& taskScheduler, const Graph& graph,
    const string& outputDirectory)
    : InMemStructuresBuilderForRels(description, taskScheduler, graph, outputDirectory) {
    if (description.hasProperties()) {
        if (description.isSingleMultiplicityPerDirection[FWD]) {
            buildInMemPropertyColumns(FWD);
        } else if (description.isSingleMultiplicityPerDirection[BWD]) {
            buildInMemPropertyColumns(BWD);
        }
    }
    buildInMemAdjColumns();
}

void InMemAdjAndPropertyColumnsBuilder::setRel(
    Direction direction, const vector<nodeID_t>& nodeIDs) {
    PageElementCursor cursor;
    calcPageElementCursor(
        description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes(),
        nodeIDs[direction].offset, cursor);
    dirLabelAdjColumns[direction][nodeIDs[direction].label]->set(cursor, nodeIDs[!direction]);
    (*directionLabelNumRels[direction])[nodeIDs[direction].label]++;
}

void InMemAdjAndPropertyColumnsBuilder::setProperty(
    const nodeID_t& nodeID, const uint32_t& propertyIdx, const uint8_t* val, const DataType& type) {
    PageElementCursor cursor;
    calcPageElementCursor(TypeUtils::getDataTypeSize(type), nodeID.offset, cursor);
    labelPropertyIdxPropertyColumn[nodeID.label][propertyIdx]->set(cursor, val);
}

void InMemAdjAndPropertyColumnsBuilder::setStringProperty(const nodeID_t& nodeID,
    const uint32_t& propertyIdx, const char* originalString, PageByteCursor& cursor) {
    gf_string_t gfString;
    labelPropertyIdxStringOverflowPages[nodeID.label][propertyIdx]
        ->setStrInOvfPageAndPtrInEncString(originalString, cursor, &gfString);
    setProperty(nodeID, propertyIdx, reinterpret_cast<uint8_t*>(&gfString), STRING);
}

void InMemAdjAndPropertyColumnsBuilder::sortOverflowStrings(LoaderProgressBar* progressBar) {
    logger->debug("Ordering String Rel Property Columns.");
    auto direction = description.isSingleMultiplicityPerDirection[FWD] ? FWD : BWD;
    for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
        labelPropertyIdxStringOverflowPages[nodeLabel].resize(description.properties.size());
        for (auto& property : description.properties) {
            if (STRING == property.dataType) {
                auto fName = RelsStore::getRelPropertyColumnFName(
                    outputDirectory, description.label, nodeLabel, property.name);
                labelPropertyIdxStringOverflowPages[nodeLabel][property.id] =
                    make_unique<InMemStringOverflowPages>(
                        StringOverflowPages::getStringOverflowPagesFName(fName));
                auto numNodes = graph.getNumNodesPerLabel()[nodeLabel];
                auto numBuckets = numNodes / 256;
                if (0 != numNodes / 256) {
                    numBuckets++;
                }
                progressBar->addAndStartNewJob(
                    "Sorting overflow string buckets for property: " +
                        graph.getCatalog().getRelLabelName(description.label) + "." + property.name,
                    numBuckets);
                node_offset_t offsetStart = 0, offsetEnd = 0;
                for (auto i = 0u; i < numBuckets; i++) {
                    offsetStart = offsetEnd;
                    offsetEnd = min(offsetStart + 256, numNodes);
                    taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                        sortOverflowStringsOfPropertyColumnTask, offsetStart, offsetEnd,
                        labelPropertyIdxPropertyColumn[nodeLabel][property.id].get(),
                        labelPropertyIdxStringOverflowPages[nodeLabel][property.id].get(),
                        labelPropertyIdxStringOverflowPages[nodeLabel][property.id].get(),
                        progressBar));
                }
                taskScheduler.waitAllTasksToCompleteOrError();
            }
        }
    }
    logger->debug("Done ordering String Rel Property Columns.");
}

void InMemAdjAndPropertyColumnsBuilder::saveToFile(LoaderProgressBar* progressBar) {
    logger->debug("Writing AdjColumns and Rel Property Columns to disk.");
    progressBar->addAndStartNewJob("Writing adjacency columns to disk for relationship: " +
                                       graph.getCatalog().getRelLabelName(description.label),
        1);
    for (auto direction : DIRECTIONS) {
        if (description.isSingleMultiplicityPerDirection[direction]) {
            for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
                taskScheduler.scheduleTask(
                    LoaderTaskFactory::createLoaderTask([&](InMemAdjPages* x) { x->saveToFile(); },
                        dirLabelAdjColumns[direction][nodeLabel].get()));
            }
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    progressBar->incrementTaskFinished();

    if (description.hasProperties() && !description.requirePropertyLists()) {
        auto direction = description.isSingleMultiplicityPerDirection[FWD] ? FWD : BWD;
        for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
            uint64_t numTasks =
                numProgressBarTasksForSavingPropertiesToDisk(description.properties);
            if (numTasks > 0) {
                progressBar->addAndStartNewJob(
                    "Writing properties to disk for relationship: " +
                        graph.getCatalog().getRelLabelName(description.label),
                    numTasks);
            }
            for (auto& property : description.properties) {
                taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                    [&](InMemPropertyPages* x, LoaderProgressBar* progressBar) {
                        x->saveToFile();
                        progressBar->incrementTaskFinished();
                    },
                    reinterpret_cast<InMemPropertyPages*>(
                        labelPropertyIdxPropertyColumn[nodeLabel][property.id].get()),
                    progressBar));
                if (STRING == property.dataType) {
                    taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                        [&](InMemStringOverflowPages* x, LoaderProgressBar* progressBar) {
                            x->saveToFile();
                            progressBar->incrementTaskFinished();
                        },
                        (labelPropertyIdxStringOverflowPages)[nodeLabel][property.id].get(),
                        progressBar));
                }
            }
            taskScheduler.waitAllTasksToCompleteOrError();
        }
    }
    logger->debug("Done writing AdjColumns and Rel Property Columns to disk.");
}

void InMemAdjAndPropertyColumnsBuilder::buildInMemPropertyColumns(Direction direction) {
    logger->debug("Creating InMemProperty Columns.");
    labelPropertyIdxPropertyColumn.resize(graph.getCatalog().getNodeLabelsCount());
    labelPropertyIdxStringOverflowPages.resize(graph.getCatalog().getNodeLabelsCount());
    for (auto& nodeLabel : description.nodeLabelsPerDirection[direction]) {
        labelPropertyIdxPropertyColumn[nodeLabel].resize((description.properties).size());
        labelPropertyIdxStringOverflowPages[nodeLabel].resize((description.properties).size());
        for (auto& property : description.properties) {
            auto fName = RelsStore::getRelPropertyColumnFName(
                outputDirectory, description.label, nodeLabel, property.name);
            auto numPages = calcNumPagesInColumn(TypeUtils::getDataTypeSize(property.dataType),
                graph.getNumNodesPerLabel()[nodeLabel]);
            labelPropertyIdxPropertyColumn[nodeLabel][property.id] =
                make_unique<InMemPropertyPages>(
                    fName, TypeUtils::getDataTypeSize(property.dataType), numPages);
            if (STRING == property.dataType) {
                labelPropertyIdxStringOverflowPages[nodeLabel][property.id] =
                    make_unique<InMemStringOverflowPages>(
                        StringOverflowPages::getStringOverflowPagesFName(fName));
            }
        }
    }
    logger->debug("Done creating InMemProperty Columns.");
}

void InMemAdjAndPropertyColumnsBuilder::buildInMemAdjColumns() {
    logger->debug("Creating InMemAdjColumns.");
    for (auto& direction : DIRECTIONS) {
        if (description.isSingleMultiplicityPerDirection[direction]) {
            dirLabelAdjColumns[direction].resize(graph.getCatalog().getNodeLabelsCount());
            directionLabelNumRels[direction] =
                make_unique<listSizes_t>(graph.getCatalog().getNodeLabelsCount());
            for (auto boundNodeLabel : description.nodeLabelsPerDirection[direction]) {
                auto fName = RelsStore::getAdjColumnFName(
                    outputDirectory, description.label, boundNodeLabel, direction);
                auto numPages = calcNumPagesInColumn(
                    description.nodeIDCompressionSchemePerDirection[direction].getNumTotalBytes(),
                    graph.getNumNodesPerLabel()[boundNodeLabel]);
                dirLabelAdjColumns[direction][boundNodeLabel] = make_unique<InMemAdjPages>(fName,
                    description.nodeIDCompressionSchemePerDirection[direction]
                        .getNumBytesForLabel(),
                    description.nodeIDCompressionSchemePerDirection[direction]
                        .getNumBytesForOffset(),
                    numPages, true /*hasNULLBytes*/);
            }
        }
    }
    logger->debug("Done creating InMemAdjColumns.");
}

void InMemAdjAndPropertyColumnsBuilder::sortOverflowStringsOfPropertyColumnTask(
    node_offset_t offsetStart, node_offset_t offsetEnd, InMemPropertyPages* propertyColumn,
    InMemStringOverflowPages* unorderedStringOverflow,
    InMemStringOverflowPages* orderedStringOverflow, LoaderProgressBar* progressBar) {
    PageByteCursor unorderedStringOverflowCursor, orderedStringOverflowCursor;
    PageElementCursor propertyListCursor;
    unorderedStringOverflowCursor.idx = 0;
    unorderedStringOverflowCursor.offset = 0;
    for (; offsetStart < offsetEnd; offsetStart++) {
        calcPageElementCursor(TypeUtils::getDataTypeSize(STRING), offsetStart, propertyListCursor);
        auto valPtr =
            reinterpret_cast<gf_string_t*>(propertyColumn->getPtrToMemLoc(propertyListCursor));
        auto len = ((uint32_t*)valPtr)[0];
        if (len > 12 && 0xffffffff != len) {
            valPtr->getOverflowPtrInfo(
                unorderedStringOverflowCursor.idx, unorderedStringOverflowCursor.offset);
            orderedStringOverflow->copyOverflowString(orderedStringOverflowCursor,
                unorderedStringOverflow->getPtrToMemLoc(unorderedStringOverflowCursor), valPtr);
        }
    }
    progressBar->incrementTaskFinished();
}

} // namespace loader
} // namespace graphflow
