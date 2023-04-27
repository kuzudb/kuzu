#include "storage/copier/node_copy_executor.h"

#include "common/string_utils.h"
#include "storage/copier/copy_task.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void NodeCopyExecutor::initializeColumnsAndLists() {
    logger->info("Initializing in memory columns.");
    for (auto& property : tableSchema->properties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(
            outputDirectory, tableSchema->tableID, property.propertyID, DBFileType::WAL_VERSION);
        if (property.dataType.typeID == SERIAL) {
            continue;
        }
        columns.push_back(
            NodeInMemColumnFactory::getNodeInMemColumn(fName, property.dataType, numRows));
        propertyIDToColumnIDMap[property.propertyID] = columns.size() - 1;
    }
    logger->info("Done initializing in memory columns.");
}

void NodeCopyExecutor::populateColumnsAndLists(processor::ExecutionContext* executionContext) {
    populateColumns(executionContext);
}

void NodeCopyExecutor::populateColumns(processor::ExecutionContext* executionContext) {
    logger->info("Populating properties");
    auto primaryKey = reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey();
    std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex;
    if (primaryKey.dataType.typeID != common::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(
            StorageUtils::getNodeIndexFName(
                this->outputDirectory, tableSchema->tableID, common::DBFileType::WAL_VERSION),
            primaryKey.dataType);
        pkIndex->bulkReserve(numRows);
    }
    std::vector<std::shared_ptr<common::Task>> tasks;
    switch (copyDescription.fileType) {
    case common::CopyDescription::FileType::CSV:
    case common::CopyDescription::FileType::PARQUET: {
        std::vector<InMemNodeColumn*> columnsToCopy(columns.size());
        for (auto i = 0u; i < columns.size(); i++) {
            columnsToCopy[i] = columns[i].get();
        }
        auto pkColumnID = propertyIDToColumnIDMap[primaryKey.propertyID];
        std::unique_ptr<NodeCopier> nodeCopier;
        if (copyDescription.fileType == common::CopyDescription::FileType::CSV) {
            auto sharedState = std::make_shared<CSVNodeCopySharedState>(copyDescription.filePaths,
                fileBlockInfos, copyDescription.csvReaderConfig.get(), tableSchema);
            nodeCopier = std::make_unique<CSVNodeCopier>(
                std::move(sharedState), pkIndex.get(), copyDescription, columnsToCopy, pkColumnID);
        } else {
            auto sharedState =
                std::make_shared<NodeCopySharedState>(copyDescription.filePaths, fileBlockInfos);
            nodeCopier = std::make_unique<ParquetNodeCopier>(
                std::move(sharedState), pkIndex.get(), copyDescription, columnsToCopy, pkColumnID);
        }
        tasks.push_back(std::make_shared<NodeCopyTask>(std::move(nodeCopier), executionContext));
    } break;
    case common::CopyDescription::FileType::NPY: {
        for (auto i = 0u; i < copyDescription.filePaths.size(); i++) {
            auto filePaths = {copyDescription.filePaths[i]};
            auto sharedState = std::make_shared<NodeCopySharedState>(filePaths, fileBlockInfos);
            // For NPY files, we can only read one column at a time.
            std::vector<InMemNodeColumn*> columnsToCopy = {columns[i].get()};
            auto nodeCopier = std::make_unique<NPYNodeCopier>(std::move(sharedState),
                i == propertyIDToColumnIDMap[primaryKey.propertyID] ? pkIndex.get() : nullptr,
                copyDescription, columnsToCopy, 0 /* pkColumnID */);
            tasks.push_back(
                std::make_shared<NodeCopyTask>(std::move(nodeCopier), executionContext));
        }
    } break;
    default: {
        throw common::CopyException(common::StringUtils::string_format("Unsupported file type {}.",
            common::CopyDescription::getFileTypeName(copyDescription.fileType)));
    }
    }
    for (auto& task : tasks) {
        taskScheduler.scheduleTaskAndWaitOrError(task, executionContext);
    }
    logger->info("Done populating properties, constructing the pk index.");
}

void NodeCopyExecutor::saveToFile() {
    logger->debug("Writing node columns to disk.");
    assert(!columns.empty());
    for (auto& column : columns) {
        taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
            [&](InMemNodeColumn* x) { x->saveToFile(); }, column.get()));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing node columns to disk.");
}

} // namespace storage
} // namespace kuzu
