#include "processor/operator/simple/load_extension.h"

#include "common/exception/io.h"

#ifdef _WIN32

#include "windows.h"
#define RTLD_NOW 0
#define RTLD_LOCAL 0

#else
#include <dlfcn.h>
#endif

#include "common/system_message.h"
#include "extension/extension.h"
#include "main/database.h"

using namespace kuzu::common;

typedef void (*ext_init_func_t)(kuzu::main::ClientContext*);

namespace kuzu {
namespace processor {

using namespace kuzu::extension;

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

void LoadExtension::executeInternal(kuzu::processor::ExecutionContext* context) {
    if (!extension::ExtensionUtils::isFullPath(path)) {
        path = ExtensionUtils::getExtensionPath(context->clientContext->getExtensionDir(), path);
    }
    auto libHdl = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (libHdl == nullptr) {
        throw common::IOException(
            stringFormat("Extension \"{}\" could not be loaded.\nError: {}", path, dlErrMessage()));
    }
    auto load = (ext_init_func_t)(dlsym(libHdl, "init"));
    if (load == nullptr) {
        throw common::IOException(
            stringFormat("Extension \"{}\" does not have a valid init function.\nError: {}", path,
                dlErrMessage()));
    }
    (*load)(context->clientContext);
}

std::string LoadExtension::getOutputMsg() {
    return stringFormat("Extension: {} has been loaded.", path);
}

} // namespace processor
} // namespace kuzu
