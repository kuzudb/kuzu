#include "processor/operator/simple/load_extension.h"

#include "common/exception/io.h"
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

#ifdef _WIN32
std::wstring utf8ToUnicode(const char* input) {
    uint32_t result;

    result = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
    if (result == 0) {
        throw IOException("Failure in MultiByteToWideChar");
    }
    auto buffer = std::make_unique<wchar_t[]>(result);
    result = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result);
    if (result == 0) {
        throw IOException("Failure in MultiByteToWideChar");
    }
    return std::wstring(buffer.get(), result);
}

void* dlopen(const char* file, int /*mode*/) {
    KU_ASSERT(file);
    auto fpath = utf8ToUnicode(file);
    return (void*)LoadLibraryW(fpath.c_str());
}

void* dlsym(void* handle, const char* name) {
    KU_ASSERT(handle);
    return (void*)GetProcAddress((HINSTANCE)handle, name);
}
#endif

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
