#include "src/loader/in_mem_builder/include/in_mem_node_builder.h"

#include "spdlog/spdlog.h"

#include "src/loader/include/loader_task.h"
#include "src/storage/storage_structure/include/in_mem_file.h"
#include "src/storage/storage_structure/include/lists/unstructured_property_lists.h"

namespace graphflow {
namespace loader {

InMemNodeBuilder::InMemNodeBuilder(const NodeFileDescription& fileDescription,
    string outputDirectory, TaskScheduler& taskScheduler, Catalog& catalog,
    LoaderProgressBar* progressBar)
    : InMemStructuresBuilder{fileDescription.labelName, fileDescription.filePath,
          move(outputDirectory), fileDescription.csvReaderConfig, taskScheduler, catalog,
          progressBar},
      IDType{fileDescription.IDType}, nodeLabel{nullptr}, numNodes{UINT64_MAX} {}

uint64_t InMemNodeBuilder::load() {
    createTableSchema();
    logger->info("Loading node {} with label {}.", labelName, nodeLabel->labelId);
    calculateNumBlocks(inputFilePath);
    auto unstructuredPropertyNames =
        countLinesPerBlockAndParseUnstrPropertyNames(nodeLabel->getNumStructuredProperties());
    numNodes = calculateNumRowsWithoutHeader();
    nodeLabel->addUnstructuredProperties(unstructuredPropertyNames);
    initializeColumnsAndList();
    // Populate structured columns with the ID hash index and count the size of unstructured lists.
    populateColumnsAndCountUnstrPropertyListSizes();
    if (unstrPropertyLists) {
        calcUnstrListsHeadersAndMetadata();
        populateUnstrPropertyLists();
    }
    saveToFile();
    logger->info("Done loading node {} with label {}.", labelName, nodeLabel->labelId);
    return numNodes;
}

void InMemNodeBuilder::createTableSchema() {
    auto propertyDefinitions = parseCSVHeader(inputFilePath);
    auto newNodeLabel = catalog.createNodeLabel(labelName, IDType, move(propertyDefinitions));
    assert(nodeLabel == nullptr);
    nodeLabel = newNodeLabel.get();
    catalog.addNodeLabel(move(newNodeLabel));
}

void InMemNodeBuilder::initializeColumnsAndList() {
    logger->info("Initializing in memory structured columns and unstructured list.");
    structuredColumns.resize(nodeLabel->getNumStructuredProperties());
    for (auto& property : nodeLabel->structuredProperties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(
            outputDirectory, nodeLabel->labelId, property.name);
        structuredColumns[property.propertyID] =
            InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numNodes);
    }
    if (nodeLabel->hasUnstructuredProperties()) {
        unstrPropertyLists = make_unique<InMemUnstructuredLists>(
            StorageUtils::getNodeUnstrPropertyListsFName(outputDirectory, nodeLabel->labelId),
            numNodes);
    }
    logger->info("Done initializing in memory structured columns and unstructured list.");
}

static vector<string> mergeUnstrPropertyNamesFromBlocks(
    vector<unordered_set<string>>& unstructuredPropertyNamesPerBlock) {
    unordered_set<string> unstructuredPropertyNames;
    for (auto& unstructuredPropertiesInBlock : unstructuredPropertyNamesPerBlock) {
        for (auto& propertyName : unstructuredPropertiesInBlock) {
            unstructuredPropertyNames.insert(propertyName);
        }
    }
    // Ensure the same order in different platforms.
    vector<string> result{unstructuredPropertyNames.begin(), unstructuredPropertyNames.end()};
    sort(result.begin(), result.end());
    return result;
}

vector<string> InMemNodeBuilder::countLinesPerBlockAndParseUnstrPropertyNames(
    uint64_t numStructuredProperties) {
    logger->info("Counting number of lines and read unstructured property names in each block.");
    vector<unordered_set<string>> unstructuredPropertyNamesPerBlock{numBlocks};
    numLinesPerBlock.resize(numBlocks);
    progressBar->addAndStartNewJob("Counting lines in the file for node: " + labelName, numBlocks);
    for (uint64_t blockId = 0; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            countNumLinesAndUnstrPropertiesPerBlockTask, inputFilePath, blockId, this,
            numStructuredProperties, &unstructuredPropertyNamesPerBlock[blockId]));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info(
        "Done counting number of lines and read unstructured property names  in each block.");
    return mergeUnstrPropertyNamesFromBlocks(unstructuredPropertyNamesPerBlock);
}

void InMemNodeBuilder::populateColumnsAndCountUnstrPropertyListSizes() {
    logger->info("Populating structured properties and Counting unstructured properties.");
    auto IDIndex = make_unique<InMemHashIndex>(
        StorageUtils::getNodeIndexFName(this->outputDirectory, nodeLabel->labelId), IDType);
    IDIndex->bulkReserve(numNodes);
    uint32_t IDColumnIdx = UINT32_MAX;
    for (auto i = 0u; i < nodeLabel->getNumStructuredProperties(); i++) {
        if (nodeLabel->structuredProperties[i].isIDProperty()) {
            IDColumnIdx = i;
            break;
        }
    }
    assert(IDColumnIdx != UINT32_MAX);
    node_offset_t offsetStart = 0;
    progressBar->addAndStartNewJob("Populating property columns for node: " + labelName, numBlocks);
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(
            LoaderTaskFactory::createLoaderTask(populateColumnsAndCountUnstrPropertyListSizesTask,
                IDColumnIdx, blockIdx, offsetStart, IDIndex.get(), this));
        offsetStart += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info("Flush the pk index to disk.");
    IDIndex->flush();
    logger->info("Done populating structured properties, constructing the pk index and counting "
                 "unstructured properties.");
}

template<DataTypeID DT>
void InMemNodeBuilder::addIDsToIndex(
    InMemColumn* column, InMemHashIndex* hashIndex, node_offset_t startOffset, uint64_t numValues) {
    assert(DT == INT64 || DT == STRING);
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if constexpr (DT == INT64) {
            auto key = *(int64_t*)column->getElement(offset);
            if (!hashIndex->append(key, offset)) {
                throw LoaderException("ID value " + to_string(key) +
                                      " violates the uniqueness constraint for the ID property.");
            }
        } else {
            auto key = ((gf_string_t*)column->getElement(offset))->getAsString();
            if (!hashIndex->append(key.c_str(), offset)) {
                throw LoaderException("ID value  " + key +
                                      " violates the uniqueness constraint for the ID property.");
            }
        }
    }
}

void InMemNodeBuilder::populateIDIndex(
    InMemColumn* column, InMemHashIndex* IDIndex, node_offset_t startOffset, uint64_t numValues) {
    switch (column->getDataType().typeID) {
    case INT64: {
        addIDsToIndex<INT64>(column, IDIndex, startOffset, numValues);
    } break;
    case STRING: {
        addIDsToIndex<STRING>(column, IDIndex, startOffset, numValues);
    } break;
    default:
        throw LoaderException("Unsupported data type " +
                              Types::dataTypeToString(column->getDataType()) +
                              " for the ID index.");
    }
}

void InMemNodeBuilder::populateColumnsAndCountUnstrPropertyListSizesTask(uint64_t IDColumnIdx,
    uint64_t blockId, uint64_t startOffset, InMemHashIndex* IDIndex, InMemNodeBuilder* builder) {
    builder->logger->trace("Start: path={0} blkIdx={1}", builder->inputFilePath, blockId);
    vector<PageByteCursor> overflowCursors(builder->nodeLabel->getNumStructuredProperties());
    CSVReader reader(builder->inputFilePath, builder->csvReaderConfig, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line.
        }
    }
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        putPropsOfLineIntoColumns(builder->structuredColumns,
            builder->nodeLabel->structuredProperties, overflowCursors, reader,
            startOffset + bufferOffset);
        if (builder->unstrPropertyLists) {
            calcLengthOfUnstrPropertyLists(
                reader, startOffset + bufferOffset, builder->unstrPropertyLists.get());
        }
        bufferOffset++;
    }
    populateIDIndex(builder->structuredColumns[IDColumnIdx].get(), IDIndex, startOffset,
        builder->numLinesPerBlock[blockId]);
    builder->progressBar->incrementTaskFinished();
    builder->logger->trace("End: path={0} blkIdx={1}", builder->inputFilePath, blockId);
}

void InMemNodeBuilder::calcLengthOfUnstrPropertyLists(
    CSVReader& reader, node_offset_t nodeOffset, InMemUnstructuredLists* unstrPropertyLists) {
    assert(unstrPropertyLists);
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        auto startPos = strchr(unstrPropertyString, ':') + 1;
        *strchr(startPos, ':') = 0;
        InMemListsUtils::incrementListSize(*unstrPropertyLists->getListSizes(), nodeOffset,
            UnstructuredPropertyLists::UNSTR_PROP_HEADER_LEN +
                Types::getDataTypeSize(Types::dataTypeFromString(string(startPos))));
    }
}

void InMemNodeBuilder::calcUnstrListsHeadersAndMetadata() {
    if (unstrPropertyLists == nullptr) {
        return;
    }
    logger->debug("Initializing UnstructuredPropertyListHeaderBuilders.");
    progressBar->addAndStartNewJob("Calculating lists headers for node: " + labelName, 1);
    taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(calculateListHeadersTask,
        numNodes, 1, unstrPropertyLists->getListSizes(),
        unstrPropertyLists->getListHeadersBuilder(), logger, progressBar));
    logger->debug("Done initializing UnstructuredPropertyListHeaders.");
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Initializing UnstructuredPropertyListsMetadata.");
    progressBar->addAndStartNewJob("Calculating lists metadata for node: " + labelName, 1);
    taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
        calculateListsMetadataAndAllocateInMemListPagesTask, numNodes, 1,
        unstrPropertyLists->getListSizes(), unstrPropertyLists->getListHeadersBuilder(),
        unstrPropertyLists.get(), false /*hasNULLBytes*/, logger, progressBar));
    logger->debug("Done initializing UnstructuredPropertyListsMetadata.");
    taskScheduler.waitAllTasksToCompleteOrError();
}

void InMemNodeBuilder::populateUnstrPropertyLists() {
    if (unstrPropertyLists == nullptr) {
        return;
    }
    logger->debug("Populating Unstructured Property Lists.");
    node_offset_t offsetStart = 0;
    progressBar->addAndStartNewJob(
        "Populating unstructured property lists for node: " + labelName, numBlocks);
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            populateUnstrPropertyListsTask, offsetStart, blockIdx, this));
        offsetStart += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done populating Unstructured Property Lists.");
}

void InMemNodeBuilder::populateUnstrPropertyListsTask(
    uint64_t blockId, node_offset_t offsetStart, InMemNodeBuilder* builder) {
    builder->logger->trace("Start: path={0} blkIdx={1}", builder->inputFilePath, blockId);
    CSVReader reader(builder->inputFilePath, builder->csvReaderConfig, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {}
    }
    auto bufferOffset = 0u;
    PageByteCursor overflowPagesCursor;
    auto unstrPropertiesNameToIdMap = builder->nodeLabel->unstrPropertiesNameToIdMap;
    assert(!unstrPropertiesNameToIdMap.empty());
    while (reader.hasNextLine()) {
        for (auto i = 0u; i < builder->nodeLabel->getNumStructuredProperties(); ++i) {
            reader.hasNextToken();
        }
        putUnstrPropsOfALineToLists(reader, offsetStart + bufferOffset, overflowPagesCursor,
            unstrPropertiesNameToIdMap,
            reinterpret_cast<InMemUnstructuredLists*>(builder->unstrPropertyLists.get()));
        bufferOffset++;
    }
    builder->progressBar->incrementTaskFinished();
    builder->logger->trace("End: path={0} blkIdx={1}", builder->inputFilePath, blockId);
}

void InMemNodeBuilder::putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>>& structuredColumns,
    const vector<Property>& structuredProperties, vector<PageByteCursor>& overflowCursors,
    CSVReader& reader, uint64_t nodeOffset) {
    for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
        reader.hasNextToken();
        auto column = structuredColumns[columnIdx].get();
        switch (column->getDataType().typeID) {
        case INT64: {
            if (!reader.skipTokenIfNull()) {
                auto int64Val = reader.getInt64();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&int64Val));
            }
        } break;
        case DOUBLE: {
            if (!reader.skipTokenIfNull()) {
                auto doubleVal = reader.getDouble();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&doubleVal));
            }
        } break;
        case BOOL: {
            if (!reader.skipTokenIfNull()) {
                auto boolVal = reader.getBoolean();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&boolVal));
            }
        } break;
        case DATE: {
            if (!reader.skipTokenIfNull()) {
                auto dateVal = reader.getDate();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&dateVal));
            }
        } break;
        case TIMESTAMP: {
            if (!reader.skipTokenIfNull()) {
                auto timestampVal = reader.getTimestamp();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&timestampVal));
            }
        } break;
        case INTERVAL: {
            if (!reader.skipTokenIfNull()) {
                auto intervalVal = reader.getInterval();
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&intervalVal));
            }
        } break;
        case STRING: {
            if (!reader.skipTokenIfNull()) {
                auto strVal = reader.getString();
                auto gfStr =
                    column->getOverflowPages()->copyString(strVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&gfStr));
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*column->getDataType().childType);
                auto gfList =
                    column->getOverflowPages()->copyList(listVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&gfList));
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

void InMemNodeBuilder::putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
    PageByteCursor& overflowPagesCursor,
    unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
    InMemUnstructuredLists* unstrPropertyLists) {
    while (reader.hasNextToken()) {
        auto unstrPropertyString = reader.getString();
        auto unstrPropertyStringBreaker1 = strchr(unstrPropertyString, ':');
        *unstrPropertyStringBreaker1 = 0;
        auto propertyKeyId = (uint32_t)unstrPropertiesNameToIdMap.at(string(unstrPropertyString));
        auto unstrPropertyStringBreaker2 = strchr(unstrPropertyStringBreaker1 + 1, ':');
        *unstrPropertyStringBreaker2 = 0;
        auto dataType = Types::dataTypeFromString(string(unstrPropertyStringBreaker1 + 1));
        auto dataTypeSize = Types::getDataTypeSize(dataType);
        auto reversePos = InMemListsUtils::decrementListSize(*unstrPropertyLists->getListSizes(),
            nodeOffset, UnstructuredPropertyLists::UNSTR_PROP_HEADER_LEN + dataTypeSize);
        PageElementCursor pageElementCursor = InMemListsUtils::calcPageElementCursor(
            unstrPropertyLists->getListHeadersBuilder()->getHeader(nodeOffset), reversePos, 1,
            nodeOffset, *unstrPropertyLists->getListsMetadataBuilder(), false /*hasNULLBytes*/);
        PageByteCursor pageCursor{pageElementCursor.pageIdx, pageElementCursor.posInPage};
        char* valuePtr = unstrPropertyStringBreaker2 + 1;
        switch (dataType.typeID) {
        case INT64: {
            auto intVal = TypeUtils::convertToInt64(valuePtr);
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                (uint8_t*)(&intVal), &overflowPagesCursor);
        } break;
        case DOUBLE: {
            auto doubleVal = TypeUtils::convertToDouble(valuePtr);
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&doubleVal), &overflowPagesCursor);
        } break;
        case BOOL: {
            auto boolVal = TypeUtils::convertToBoolean(valuePtr);
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&boolVal), &overflowPagesCursor);
        } break;
        case DATE: {
            char* beginningOfDateStr = valuePtr;
            date_t dateVal = Date::FromCString(beginningOfDateStr, strlen(beginningOfDateStr));
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&dateVal), &overflowPagesCursor);
        } break;
        case TIMESTAMP: {
            char* beginningOfTimestampStr = valuePtr;
            timestamp_t timestampVal =
                Timestamp::FromCString(beginningOfTimestampStr, strlen(beginningOfTimestampStr));
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&timestampVal), &overflowPagesCursor);
        } break;
        case INTERVAL: {
            char* beginningOfIntervalStr = valuePtr;
            interval_t intervalVal =
                Interval::FromCString(beginningOfIntervalStr, strlen(beginningOfIntervalStr));
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(&intervalVal), &overflowPagesCursor);
        } break;
        case STRING: {
            unstrPropertyLists->setUnstructuredElement(pageCursor, propertyKeyId, dataType.typeID,
                reinterpret_cast<uint8_t*>(valuePtr), &overflowPagesCursor);
        } break;
        default:
            throw LoaderException("unsupported dataType while parsing unstructured property");
        }
    }
}

void InMemNodeBuilder::saveToFile() {
    logger->debug("Writing node structured columns to disk.");
    assert(!structuredColumns.empty());
    progressBar->addAndStartNewJob(
        "Saving structured columns and unstructured lists to disk for node: " + labelName,
        structuredColumns.size() + (unstrPropertyLists != nullptr));
    for (auto& column : structuredColumns) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            [&](InMemColumn* x, LoaderProgressBar* progressBar_) {
                x->saveToFile();
                progressBar_->incrementTaskFinished();
            },
            column.get(), progressBar));
    }
    if (unstrPropertyLists) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            [&](InMemLists* x, LoaderProgressBar* progressBar_) {
                x->saveToFile();
                progressBar_->incrementTaskFinished();
            },
            unstrPropertyLists.get(), progressBar));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing node structured columns to disk.");
}

} // namespace loader
} // namespace graphflow
