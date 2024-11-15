#include "main/settings.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace main {

void SpillToDiskSetting::setContext(ClientContext* context, const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getDBConfigUnsafe()->enableSpillingToDisk = parameter.getValue<bool>();
    const auto& dbConfig = *context->getDBConfig();
    std::string spillPath;
    if (dbConfig.enableSpillingToDisk) {
        if (dbConfig.isDBPathInMemory(context->getDatabasePath())) {
            throw common::RuntimeException(
                "Cannot set spill_to_disk to true for an in-memory database!");
        }
        if (!context->canExecuteWriteQuery()) {
            throw common::RuntimeException(
                "Cannot set spill_to_disk to true for a read only database!");
        }
        spillPath = context->getVFSUnsafe()->joinPath(context->getDatabasePath(),
            common::StorageConstants::TEMP_SPILLING_FILE_NAME);
    } else {
        // Set path to empty will disable spiller.
        spillPath = "";
    }
    context->getMemoryManager()->getBufferManager()->resetSpiller(spillPath);
}

} // namespace main
} // namespace kuzu
