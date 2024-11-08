#include "main/settings.h"

#include "main/client_context.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace main {
void SpillToDiskFileSetting::setContext(ClientContext* context, const common::Value& parameter) {
    parameter.validateType(inputType);
    context->getDBConfigUnsafe()->spillToDiskTmpFile = parameter.getValue<std::string>();
    const auto& dbConfig = *context->getDBConfig();
    context->getMemoryManager()->getBufferManager()->resetSpiller(dbConfig);
}

} // namespace main
} // namespace kuzu
