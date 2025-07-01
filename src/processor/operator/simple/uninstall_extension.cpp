#include "processor/operator/simple/uninstall_extension.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "extension/extension.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::extension;

void UninstallExtension::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto vfs = clientContext->getVFSUnsafe();
    auto localLibFilePath = ExtensionUtils::getLocalPathForExtensionLib(clientContext, path);
    if (!vfs->fileOrPathExists(localLibFilePath)) {
        throw RuntimeException{
            stringFormat("Can not uninstall extension: {} since it has not been installed.", path)};
    }
    std::error_code errCode;
    if (!std::filesystem::remove_all(
            extension::ExtensionUtils::getLocalDirForExtension(clientContext, path), errCode)) {
        // LCOV_EXCL_START
        throw RuntimeException{
            stringFormat("An error occurred while uninstalling extension: {}. Error: {}.", path,
                errCode.message())};
        // LCOV_EXCL_STOP
    }
    appendMessage(stringFormat("Extension: {} has been uninstalled", path),
        clientContext->getMemoryManager());
}

} // namespace processor
} // namespace kuzu
