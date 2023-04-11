#include "storage/copier/npy_node_copier.h"

#include "common/string_utils.h"
#include "storage/copier/copy_task.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void NpyNodeCopier::populateInMemoryStructures(processor::ExecutionContext* executionContext) {
    initializeNpyReaders();
    initializeColumnsAndLists();
    validateNpyReaders();
    populateColumnsAndLists(executionContext);
}

void NpyNodeCopier::initializeNpyReaders() {
    logger->info("Initializing npy readers.");
    for (auto& property : tableSchema->properties) {
        auto npyPath = copyDescription.propertyIDToNpyMap[property.propertyID];
        npyReaderMap[property.propertyID] = std::make_unique<NpyReader>(npyPath);
    }
    assert(!npyReaderMap.empty());
    numRows = npyReaderMap.begin()->second->getLength();
    logger->info("Done initializing npy readers.");
}

void NpyNodeCopier::validateNpyReaders() {
    for (auto& [_, npyReader] : npyReaderMap) {
        auto length = npyReader->getLength();
        if (length == 0) {
            throw Exception(StringUtils::string_format(
                "Number of rows in npy file {} is 0.", npyReader->getFileName()));
        } else if (length != numRows) {
            throw Exception("Number of rows in npy files is not equal to each other.");
        }
    }
    for (auto& property : tableSchema->properties) {
        auto& npyReader = npyReaderMap[property.propertyID];
        if (property.dataType.typeID == npyReader->getType()) {
            if (npyReader->getNumElementsPerRow() != 1) {
                throw Exception(
                    StringUtils::string_format("Cannot copy a vector property in npy file {} to a "
                                               "scalar property in table {}.",
                        npyReader->getFileName(), tableSchema->tableName));
            }
            continue;
        } else if (property.dataType.typeID == common::DataTypeID::FIXED_LIST) {
            if (npyReader->getType() != property.dataType.getChildType()->typeID) {
                throw Exception(StringUtils::string_format("The type of npy file {} does not "
                                                           "match the type defined in table {}.",
                    npyReader->getFileName(), tableSchema->tableName));
            }
            auto fixedListInfo =
                reinterpret_cast<FixedListTypeInfo*>(property.dataType.getExtraTypeInfo());
            if (npyReader->getNumElementsPerRow() != fixedListInfo->getFixedNumElementsInList()) {
                throw Exception(
                    StringUtils::string_format("The shape of {} does not match the length of the "
                                               "fixed list property in table "
                                               "{}.",
                        npyReader->getFileName(), tableSchema->tableName));
            }
            continue;
        } else {
            throw Exception(StringUtils::string_format("The type of npy file {} does not "
                                                       "match the type defined in table {}.",
                npyReader->getFileName(), tableSchema->tableName));
        }
    }
}

void NpyNodeCopier::populateColumnsAndLists(processor::ExecutionContext* executionContext) {
    logger->info("Populating properties");
    auto primaryKey = reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey();
    if (primaryKey.dataType.typeID != INT64) {
        throw CopyException(StringUtils::string_format(
            "Data type {} for the ID index is not currently supported when copying from npy file.",
            Types::dataTypeToString(primaryKey.dataType)));
    }
    auto pkIndex = std::make_unique<HashIndexBuilder<int64_t>>(
        StorageUtils::getNodeIndexFName(
            this->outputDirectory, tableSchema->tableID, DBFileType::WAL_VERSION),
        reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey().dataType);
    pkIndex->bulkReserve(numRows);
    populateColumnsFromNpy(pkIndex);
    logger->info("Flush the pk index to disk.");
    pkIndex->flush();
    logger->info("Done populating properties, constructing the pk index.");
}

void NpyNodeCopier::populateColumnsFromNpy(std::unique_ptr<HashIndexBuilder<int64_t>>& pkIndex) {
    for (auto& [propertyID, _] : columns) {
        assignCopyNpyTasks(propertyID, pkIndex);
    }
}

void NpyNodeCopier::assignCopyNpyTasks(
    common::property_id_t propertyID, std::unique_ptr<HashIndexBuilder<int64_t>>& pkIndex) {
    auto& npyReader = npyReaderMap[propertyID];
    offset_t currRowIdx = 0;
    size_t blockIdx;
    while (currRowIdx < npyReader->getLength()) {
        for (int i = 0; i < common::CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (currRowIdx >= npyReader->getLength()) {
                break;
            }
            auto numLinesInCurBlock =
                CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY > npyReader->getLength() - currRowIdx ?
                    npyReader->getLength() - currRowIdx :
                    CopyConstants::NUM_ROWS_PER_BLOCK_FOR_NPY;
            auto primaryKeyId =
                reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey().propertyID;
            taskScheduler.scheduleTask(
                CopyTaskFactory::createCopyTask(batchPopulateColumnsTask, primaryKeyId, blockIdx,
                    currRowIdx, numLinesInCurBlock, pkIndex.get(), this, propertyID));
            currRowIdx += numLinesInCurBlock;
            blockIdx += 1;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConstants::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }
    taskScheduler.waitAllTasksToCompleteOrError();
}

void NpyNodeCopier::batchPopulateColumnsTask(common::property_id_t primaryKeypropertyID,
    uint64_t blockIdx, offset_t startNodeOffset, uint64_t numLinesInCurBlock,
    HashIndexBuilder<int64_t>* pkIndex, NpyNodeCopier* copier, common::property_id_t propertyID) {
    auto& npyReader = copier->npyReaderMap[propertyID];
    copier->logger->trace("Start: path={0} blkIdx={1}", npyReader->getFileName(), blockIdx);

    // Create a column chunk for tuples within the [StartOffset, endOffset] range.
    auto endNodeOffset = startNodeOffset + numLinesInCurBlock - 1;
    auto& column = copier->columns[propertyID];
    std::unique_ptr<InMemColumnChunk> columnChunk =
        std::make_unique<InMemColumnChunk>(startNodeOffset, endNodeOffset,
            column->getNumBytesForElement(), column->getNumElementsInAPage());
    for (auto i = startNodeOffset; i < startNodeOffset + numLinesInCurBlock; ++i) {
        void* data = npyReader->getPointerToRow(i);
        column->setElementInChunk(columnChunk.get(), i, (uint8_t*)data);
    }
    column->flushChunk(columnChunk.get(), startNodeOffset, endNodeOffset);

    if (propertyID == primaryKeypropertyID) {
        auto pkColumn = copier->columns.at(primaryKeypropertyID).get();
        populatePKIndex(columnChunk.get(), pkColumn->getInMemOverflowFile(),
            pkColumn->getNullMask(), pkIndex, startNodeOffset, numLinesInCurBlock);
    }

    copier->logger->trace("End: path={0} blkIdx={1}", npyReader->getFileName(), blockIdx);
}
} // namespace storage
} // namespace kuzu
