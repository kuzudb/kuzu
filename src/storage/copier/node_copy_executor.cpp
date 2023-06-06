#include "storage/copier/node_copy_executor.h"

#include "common/string_utils.h"
#include "storage/copier/copy_task.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

offset_t NodeCopyExecutor::copy(processor::ExecutionContext* executionContext) {
    numRows = TableCopyUtils::countNumLines(copyDescription, tableSchema, fileBlockInfos);
    populateColumns(executionContext);
    nodesStatisticsAndDeletedIDs->setNumTuplesForTable(tableSchema->tableID, numRows);
    return numRows;
}

void NodeCopyExecutor::populateColumns(processor::ExecutionContext* executionContext) {
    std::vector<std::shared_ptr<common::Task>> tasks;
    switch (copyDescription.fileType) {
    case common::CopyDescription::FileType::NPY: {
        assert(copyDescription.filePaths.size() == tableSchema->properties.size());
        for (auto i = 0u; i < copyDescription.filePaths.size(); i++) {
            auto filePaths = {copyDescription.filePaths[i]};
            auto sharedState = std::make_shared<CopySharedState>(filePaths, fileBlockInfos);
            // For NPY files, we can only read one column at a time.
            auto nodeCopier = std::make_unique<NPYNodeCopier>(
                outputDirectory, std::move(sharedState), copyDescription, tableSchema, numRows, i);
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
