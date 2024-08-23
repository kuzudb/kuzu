#include "extension/extension.h"

#include "catalog/catalog.h"
#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "common/system_message.h"
#include "function/table_functions.h"
#include "main/client_context.h"
#include "main/database.h"
#include "transaction/transaction.h"
#ifdef _WIN32

#include "windows.h"
#define RTLD_NOW 0
#define RTLD_LOCAL 0

#else
#include <dlfcn.h>
#endif

namespace kuzu {
namespace extension {

std::string getOS() {
    std::string os = "linux";
#if !defined(_GLIBCXX_USE_CXX11_ABI) || _GLIBCXX_USE_CXX11_ABI == 0
    if (os == "linux") {
        os = "linux_old";
    }
#endif
#ifdef _WIN32
    os = "win";
#elif defined(__APPLE__)
    os = "osx";
#endif
    return os;
}

std::string getArch() {
    std::string arch = "amd64";
#if defined(__i386__) || defined(_M_IX86)
    arch = "x86";
#elif defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
    arch = "arm64";
#endif
    return arch;
}

std::string getPlatform() {
    return getOS() + "_" + getArch();
}

bool ExtensionUtils::isFullPath(const std::string& extension) {
    return extension.find('.') != std::string::npos || extension.find('/') != std::string::npos ||
           extension.find('\\') != std::string::npos;
}

static ExtensionRepoInfo getExtensionRepoInfo(std::string& extensionURL) {
    common::StringUtils::replaceAll(extensionURL, "http://", "");
    auto hostNamePos = extensionURL.find('/');
    auto hostName = extensionURL.substr(0, hostNamePos);
    auto hostURL = "http://" + hostName;
    auto hostPath = extensionURL.substr(hostNamePos);
    return {hostPath, hostURL, extensionURL};
};

ExtensionRepoInfo ExtensionUtils::getExtensionLibRepoInfo(const std::string& extensionName) {
    auto extensionURL = common::stringFormat(EXTENSION_FILE_REPO, KUZU_EXTENSION_VERSION,
        getPlatform(), extensionName, getExtensionFileName(extensionName));
    return getExtensionRepoInfo(extensionURL);
}

ExtensionRepoInfo ExtensionUtils::getExtensionLoaderRepoInfo(const std::string& extensionName) {
    auto extensionURL =
        common::stringFormat(EXTENSION_FILE_REPO, KUZU_EXTENSION_VERSION, getPlatform(),
            extensionName, getExtensionFileName(extensionName + EXTENSION_LOADER_SUFFIX));
    return getExtensionRepoInfo(extensionURL);
}

ExtensionRepoInfo ExtensionUtils::getExtensionInstallerRepoInfo(const std::string& extensionName) {
    auto extensionURL =
        common::stringFormat(EXTENSION_FILE_REPO, KUZU_EXTENSION_VERSION, getPlatform(),
            extensionName, getExtensionFileName(extensionName + EXTENSION_INSTALLER_SUFFIX));
    return getExtensionRepoInfo(extensionURL);
}

ExtensionRepoInfo ExtensionUtils::getSharedLibRepoInfo(const std::string& fileName) {
    auto extensionURL =
        common::stringFormat(SHARED_LIB_REPO, KUZU_EXTENSION_VERSION, getPlatform(), fileName);
    return getExtensionRepoInfo(extensionURL);
}

std::string ExtensionUtils::getExtensionFileName(const std::string& name) {
    return common::stringFormat(EXTENSION_FILE_NAME, common::StringUtils::getLower(name));
}

std::string ExtensionUtils::getLocalPathForExtensionLib(main::ClientContext* context,
    const std::string& extensionName) {
    return common::stringFormat("{}{}/{}", context->getExtensionDir(), extensionName,
        getExtensionFileName(extensionName));
}

std::string ExtensionUtils::getLocalPathForExtensionLoader(main::ClientContext* context,
    const std::string& extensionName) {
    return common::stringFormat("{}{}/{}", context->getExtensionDir(), extensionName,
        getExtensionFileName(extensionName + EXTENSION_LOADER_SUFFIX));
}

std::string ExtensionUtils::getLocalPathForExtensionInstaller(main::ClientContext* context,
    const std::string& extensionName) {
    return common::stringFormat("{}{}/{}", context->getExtensionDir(), extensionName,
        getExtensionFileName(extensionName + EXTENSION_INSTALLER_SUFFIX));
}

std::string ExtensionUtils::getLocalExtensionDir(main::ClientContext* context,
    const std::string& extensionName) {
    return common::stringFormat("{}{}", context->getExtensionDir(), extensionName);
}

void ExtensionUtils::registerTableFunction(main::Database& database,
    std::unique_ptr<function::TableFunction> function) {
    auto name = function->name;
    function::function_set functionSet;
    functionSet.push_back(std::move(function));
    auto catalog = database.getCatalog();
    if (catalog->getFunctions(&transaction::DUMMY_TRANSACTION)
            ->containsEntry(&transaction::DUMMY_TRANSACTION, name)) {
        return;
    }
    catalog->addFunction(&transaction::DUMMY_TRANSACTION,
        catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY, std::move(name), std::move(functionSet));
}

std::string ExtensionUtils::appendLibSuffix(const std::string& libName) {
    auto os = getOS();
    std::string suffix;
    if (os == "linux" || os == "linux_old") {
        suffix = "so";
    } else if (os == "osx") {
        suffix = "dylib";
    } else {
        KU_UNREACHABLE;
    }
    return common::stringFormat("{}.{}", libName, suffix);
}

std::string ExtensionUtils::getLocalPathForSharedLib(main::ClientContext* context,
    const std::string& libName) {
    return common::stringFormat("{}common/{}", context->getExtensionDir(), libName);
}

std::string ExtensionUtils::getLocalPathForSharedLib(main::ClientContext* context) {
    return common::stringFormat("{}common/", context->getExtensionDir());
}

void ExtensionUtils::registerFunctionSet(main::Database& database, std::string name,
    function::function_set functionSet) {
    auto catalog = database.getCatalog();
    if (catalog->getFunctions(&transaction::DUMMY_TRANSACTION)
            ->containsEntry(&transaction::DUMMY_TRANSACTION, name)) {
        return;
    }
    catalog->addFunction(&transaction::DUMMY_TRANSACTION,
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY, std::move(name), std::move(functionSet));
}

bool ExtensionUtils::isOfficialExtension(const std::string& extension) {
    auto extensionUpperCase = common::StringUtils::getUpper(extension);
    for (auto& officialExtension : OFFICIAL_EXTENSION) {
        if (officialExtension == extensionUpperCase) {
            return true;
        }
    }
    return false;
}

ExtensionLibLoader::ExtensionLibLoader(const std::string& extensionName, const std::string& path)
    : extensionName{extensionName} {
    libHdl = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (libHdl == nullptr) {
        throw common::IOException(common::stringFormat(
            "Failed to load library: {} which is needed by extension: {}.\nError: {}.", path,
            extensionName, common::dlErrMessage()));
    }
}

ext_load_func_t ExtensionLibLoader::getLoadFunc() {
    return (ext_load_func_t)getDynamicLibFunc(EXTENSION_LOAD_FUNC_NAME);
}

ext_init_func_t ExtensionLibLoader::getInitFunc() {
    return (ext_init_func_t)getDynamicLibFunc(EXTENSION_INIT_FUNC_NAME);
}

ext_install_func_t ExtensionLibLoader::getInstallFunc() {
    return (ext_install_func_t)getDynamicLibFunc(EXTENSION_INSTALL_FUNC_NAME);
}

void* ExtensionLibLoader::getDynamicLibFunc(const std::string& funcName) {
    auto sym = dlsym(libHdl, funcName.c_str());
    if (sym == nullptr) {
        throw common::IOException(
            common::stringFormat("Failed to load {} function in extension {}.\nError: {}", funcName,
                extensionName, common::dlErrMessage()));
    }
    return sym;
}

void ExtensionOptions::addExtensionOption(std::string name, common::LogicalTypeID type,
    common::Value defaultValue) {
    common::StringUtils::toLower(name);
    extensionOptions.emplace(name, main::ExtensionOption{name, type, std::move(defaultValue)});
}

main::ExtensionOption* ExtensionOptions::getExtensionOption(std::string name) {
    common::StringUtils::toLower(name);
    return extensionOptions.contains(name) ? &extensionOptions.at(name) : nullptr;
}

#ifdef _WIN32
std::wstring utf8ToUnicode(const char* input) {
    uint32_t result;

    result = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
    if (result == 0) {
        throw common::IOException("Failure in MultiByteToWideChar");
    }
    auto buffer = std::make_unique<wchar_t[]>(result);
    result = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result);
    if (result == 0) {
        throw common::IOException("Failure in MultiByteToWideChar");
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

} // namespace extension
} // namespace kuzu
