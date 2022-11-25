#include "storage/in_mem_csv_copier/in_mem_node_csv_copier.h"

#include "spdlog/spdlog.h"
#include "storage/in_mem_csv_copier/copy_csv_task.h"
#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

InMemNodeCSVCopier::InMemNodeCSVCopier(CSVDescription& csvDescription, string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
    : InMemStructuresCSVCopier{csvDescription, move(outputDirectory), taskScheduler, catalog},
      numNodes{UINT64_MAX}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {
    nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(tableID);
}

uint64_t InMemNodeCSVCopier::copy() {
    logger->info(
        "Copying node {} with table {}.", nodeTableSchema->tableName, nodeTableSchema->tableID);
    calculateNumBlocks(csvDescription.filePath, nodeTableSchema->tableName);
    countLinesPerBlock(nodeTableSchema->getNumStructuredProperties());
    numNodes = calculateNumRows(csvDescription.csvReaderConfig.hasHeader);
    initializeColumnsAndList();
    // Populate structured columns with the ID hash index.
    switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
    case INT64: {
        populateColumns<int64_t>();
    } break;
    case STRING: {
        populateColumns<ku_string_t>();
    } break;
    default: {
        throw CopyCSVException("Unsupported data type " +
                               Types::dataTypeToString(nodeTableSchema->getPrimaryKey().dataType) +
                               " for the ID index.");
    }
    }
    saveToFile();
    nodesStatisticsAndDeletedIDs->setNumTuplesForTable(nodeTableSchema->tableID, numNodes);
    logger->info("Done copying node {} with table {}.", nodeTableSchema->tableName,
        nodeTableSchema->tableID);
    return numNodes;
}

void InMemNodeCSVCopier::initializeColumnsAndList() {
    logger->info("Initializing in memory structured columns.");
    structuredColumns.resize(nodeTableSchema->getNumStructuredProperties());
    for (auto& property : nodeTableSchema->structuredProperties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(outputDirectory,
            nodeTableSchema->tableID, property.propertyID, DBFileType::WAL_VERSION);
        structuredColumns[property.propertyID] =
            InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numNodes);
    }
    logger->info("Done initializing in memory structured columns.");
}

void InMemNodeCSVCopier::countLinesPerBlock(uint64_t numStructuredProperties) {
    logger->info("Counting number of lines in each block.");
    numLinesPerBlock.resize(numBlocks);
    for (uint64_t blockId = 0; blockId < numBlocks; blockId++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            countNumLinesPerBlockTask, csvDescription.filePath, blockId, this));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info("Done counting number of lines in each block.");
}

template<typename T>
void InMemNodeCSVCopier::populateColumns() {
    logger->info("Populating structured properties.");
    auto pkIndex =
        make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                             nodeTableSchema->tableID, DBFileType::WAL_VERSION),
            nodeTableSchema->getPrimaryKey().dataType);
    pkIndex->bulkReserve(numNodes);
    node_offset_t offsetStart = 0;
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(populateColumnsTask<T>,
            nodeTableSchema->primaryKeyPropertyIdx, blockIdx, offsetStart, pkIndex.get(), this));
        offsetStart += numLinesPerBlock[blockIdx];
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->info("Flush the pk index to disk.");
    pkIndex->flush();
    logger->info("Done populating structured properties, constructing the pk index.");
}

template<typename T>
void InMemNodeCSVCopier::addIDsToIndex(InMemColumn* column, HashIndexBuilder<T>* hashIndex,
    node_offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if constexpr (is_same<T, int64_t>::value) {
            auto key = (int64_t*)column->getElement(offset);
            if (!hashIndex->append(*key, offset)) {
                throw CopyCSVException(Exception::getExistedPKExceptionMsg(to_string(*key)));
            }
        } else {
            auto element = (ku_string_t*)column->getElement(offset);
            auto key = column->getInMemOverflowFile()->readString(element);
            if (!hashIndex->append(key.c_str(), offset)) {
                throw CopyCSVException(Exception::getExistedPKExceptionMsg(key));
            }
        }
    }
}

template<typename T>
void InMemNodeCSVCopier::populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
    node_offset_t startOffset, uint64_t numValues) {
    addIDsToIndex(column, pkIndex, startOffset, numValues);
}

void InMemNodeCSVCopier::skipFirstRowIfNecessary(
    uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader) {
    if (0 == blockId && csvDescription.csvReaderConfig.hasHeader && reader.hasNextLine()) {
        reader.skipLine();
    }
}

template<typename T>
void InMemNodeCSVCopier::populateColumnsTask(uint64_t primaryKeyPropertyIdx, uint64_t blockId,
    uint64_t offsetStart, HashIndexBuilder<T>* pkIndex, InMemNodeCSVCopier* copier) {
    copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
    vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumStructuredProperties());
    CSVReader reader(
        copier->csvDescription.filePath, copier->csvDescription.csvReaderConfig, blockId);
    skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
    auto bufferOffset = 0u;
    while (reader.hasNextLine()) {
        putPropsOfLineIntoColumns(copier->structuredColumns,
            copier->nodeTableSchema->structuredProperties, overflowCursors, reader,
            offsetStart + bufferOffset);
        bufferOffset++;
    }
    populatePKIndex(copier->structuredColumns[primaryKeyPropertyIdx].get(), pkIndex, offsetStart,
        copier->numLinesPerBlock[blockId]);
    copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
}

void InMemNodeCSVCopier::putPropsOfLineIntoColumns(
    vector<unique_ptr<InMemColumn>>& structuredColumns,
    const vector<Property>& structuredProperties, vector<PageByteCursor>& overflowCursors,
    CSVReader& reader, uint64_t nodeOffset) {
    for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
        reader.hasNextTokenOrError();
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
                auto kuStr =
                    column->getInMemOverflowFile()->copyString(strVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&kuStr));
            }
        } break;
        case LIST: {
            if (!reader.skipTokenIfNull()) {
                auto listVal = reader.getList(*column->getDataType().childType);
                auto kuList =
                    column->getInMemOverflowFile()->copyList(listVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&kuList));
            }
        } break;
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
    }
}

void InMemNodeCSVCopier::saveToFile() {
    logger->debug("Writing node structured columns to disk.");
    assert(!structuredColumns.empty());
    for (auto& column : structuredColumns) {
        taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
            [&](InMemColumn* x) { x->saveToFile(); }, column.get()));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing node structured columns to disk.");
}

} // namespace storage
} // namespace kuzu
