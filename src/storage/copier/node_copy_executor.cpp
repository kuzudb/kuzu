#include "storage/copier/node_copy_executor.h"

#include "common/string_utils.h"
#include "storage/copier/copy_task.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void NodeCopyExecutor::populateColumnsAndLists(processor::ExecutionContext* executionContext) {
    populateColumns(executionContext);
}

static column_id_t getPKColumnID(
    const std::vector<Property>& properties, property_id_t pkPropertyID) {
    column_id_t pkColumnID = 0;
    for (auto& property : properties) {
        if (property.propertyID == pkPropertyID) {
            break;
        }
        pkColumnID++;
    }
    return pkColumnID;
}

void NodeCopyExecutor::populateColumns(processor::ExecutionContext* executionContext) {
    auto primaryKey = reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey();
    std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex;
    if (primaryKey.dataType.getLogicalTypeID() != common::LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(
            StorageUtils::getNodeIndexFName(
                this->outputDirectory, tableSchema->tableID, common::DBFileType::WAL_VERSION),
            primaryKey.dataType);
        pkIndex->bulkReserve(numRows);
    }
    auto pkColumnID = getPKColumnID(tableSchema->properties, primaryKey.propertyID);
    std::vector<std::shared_ptr<common::Task>> tasks;
    switch (copyDescription.fileType) {
    case common::CopyDescription::FileType::CSV:
    case common::CopyDescription::FileType::PARQUET: {
        std::unique_ptr<NodeCopier> nodeCopier;
        if (copyDescription.fileType == common::CopyDescription::FileType::CSV) {
            auto sharedState = std::make_shared<CSVNodeCopySharedState>(copyDescription.filePaths,
                fileBlockInfos, copyDescription.csvReaderConfig.get(), tableSchema);
            nodeCopier = std::make_unique<CSVNodeCopier>(outputDirectory, std::move(sharedState),
                pkIndex.get(), copyDescription, tableSchema, table, pkColumnID);
        } else {
            auto sharedState =
                std::make_shared<NodeCopySharedState>(copyDescription.filePaths, fileBlockInfos);
            nodeCopier =
                std::make_unique<ParquetNodeCopier>(outputDirectory, std::move(sharedState),
                    pkIndex.get(), copyDescription, tableSchema, table, pkColumnID);
        }
        tasks.push_back(std::make_shared<NodeCopyTask>(std::move(nodeCopier), executionContext));
    } break;
    case common::CopyDescription::FileType::NPY: {
        assert(copyDescription.filePaths.size() == tableSchema->properties.size());
        for (auto i = 0u; i < copyDescription.filePaths.size(); i++) {
            auto filePaths = {copyDescription.filePaths[i]};
            auto propertyID = tableSchema->properties[i].propertyID;
            auto sharedState = std::make_shared<NodeCopySharedState>(filePaths, fileBlockInfos);
            // For NPY files, we can only read one column at a time.
            auto nodeCopier =
                std::make_unique<NPYNodeCopier>(outputDirectory, std::move(sharedState),
                    propertyID == primaryKey.propertyID ? pkIndex.get() : nullptr, copyDescription,
                    tableSchema, table, 0 /* pkColumnID */, i);
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
}

} // namespace storage
} // namespace kuzu
