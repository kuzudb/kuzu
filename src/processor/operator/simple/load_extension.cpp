#include "processor/operator/simple/load_extension.h"

#include "common/file_system/virtual_file_system.h"
#include "extension/extension.h"
#include "main/database.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

using namespace kuzu::extension;

std::string LoadExtensionPrintInfo::toString() const {
    return "Load " + extensionName;
}

static void executeExtensionLoader(main::ClientContext* context, const std::string& extensionName) {
    auto loaderPath = ExtensionUtils::getLocalPathForExtensionLoader(context, extensionName);
    if (context->getVFSUnsafe()->fileOrPathExists(loaderPath)) {
        auto libLoader = ExtensionLibLoader(extensionName, loaderPath);
        auto load = libLoader.getLoadFunc();
        (*load)(context);
    }
}

void LoadExtension::executeInternal(kuzu::processor::ExecutionContext* context) {
    auto fullPath = path;
    if (!extension::ExtensionUtils::isFullPath(path)) {
        auto localPathForSharedLib =
            ExtensionUtils::getLocalPathForSharedLib(context->clientContext);
        if (!context->clientContext->getVFSUnsafe()->fileOrPathExists(localPathForSharedLib)) {
            context->clientContext->getVFSUnsafe()->createDir(localPathForSharedLib);
        }
        executeExtensionLoader(context->clientContext, path);
        fullPath = ExtensionUtils::getLocalPathForExtensionLib(context->clientContext, path);
    }

    auto libLoader = ExtensionLibLoader(path, fullPath);
    auto init = libLoader.getInitFunc();
    (*init)(context->clientContext);
}

std::string LoadExtension::getOutputMsg() {
    return stringFormat("Extension: {} has been loaded.", path);
}

} // namespace processor
} // namespace kuzu
