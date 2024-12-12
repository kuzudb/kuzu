#pragma once

#include <string>
#include <unordered_map>

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "common/api.h"
#include "function/function.h"
#include "main/db_config.h"

#define ADD_FUNC_BASE(_PARAM, _NAME, _TYPE)                                                        \
    kuzu::extension::ExtensionUtils::registerFunctionSet(db, std::string(_NAME),                   \
        _PARAM::getFunctionSet(), _TYPE)

#define ADD_FUNC(FUNC_STRUCT, FUNC_TYPE) ADD_FUNC_BASE(FUNC_STRUCT, FUNC_STRUCT::name, FUNC_TYPE)

#define ADD_FUNC_ALIAS(FUNC_STRUCT, FUNC_TYPE)                                                     \
    ADD_FUNC_BASE(FUNC_STRUCT::alias, FUNC_STRUCT::name, FUNC_TYPE)

#define ADD_SCALAR_FUNC(FUNC_STRUCT)                                                               \
    ADD_FUNC(FUNC_STRUCT, catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY)

#define ADD_SCALAR_FUNC_ALIAS(FUNC_STRUCT)                                                         \
    ADD_FUNC_ALIAS(FUNC_STRUCT, catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY)

#define ADD_GDS_FUNC(FUNC_STRUCT)                                                                  \
    ADD_FUNC(FUNC_STRUCT, catalog::CatalogEntryType::GDS_FUNCTION_ENTRY)

#define ADD_TABLE_FUNC(FUNC_STRUCT)                                                                \
    ADD_FUNC(FUNC_STRUCT, catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY)

#define ADD_EXTENSION_OPTION(OPTION)                                                               \
    db->addExtensionOption(OPTION::NAME, OPTION::TYPE, OPTION::getDefaultValue())

namespace kuzu {
namespace function {
struct TableFunction;
} // namespace function
namespace main {
class Database;
} // namespace main

namespace extension {

typedef void (*ext_init_func_t)(kuzu::main::ClientContext*);
using ext_load_func_t = ext_init_func_t;
using ext_install_func_t = ext_init_func_t;

std::string getPlatform();

class KUZU_API Extension {
public:
    virtual ~Extension() = default;
};

struct ExtensionRepoInfo {
    std::string hostPath;
    std::string hostURL;
    std::string repoURL;
};

struct ExtensionUtils {
    static constexpr const char* EXTENSION_FILE_REPO = "http://extension.kuzudb.com/v{}/{}/{}/{}";

    static constexpr const char* SHARED_LIB_REPO = "http://extension.kuzudb.com/v{}/{}/common/{}";

    static constexpr const char* EXTENSION_FILE_NAME = "lib{}.kuzu_extension";

    static constexpr const char* OFFICIAL_EXTENSION[] = {"HTTPFS", "POSTGRES", "DUCKDB", "JSON",
        "SQLITE"};

    static constexpr const char* EXTENSION_LOADER_SUFFIX = "_loader";

    static constexpr const char* EXTENSION_INSTALLER_SUFFIX = "_installer";

    static bool isFullPath(const std::string& extension);

    static KUZU_API ExtensionRepoInfo getExtensionLibRepoInfo(const std::string& extensionName);

    static KUZU_API ExtensionRepoInfo getExtensionLoaderRepoInfo(const std::string& extensionName);

    static KUZU_API ExtensionRepoInfo getExtensionInstallerRepoInfo(
        const std::string& extensionName);

    static KUZU_API ExtensionRepoInfo getSharedLibRepoInfo(const std::string& fileName);

    static std::string getExtensionFileName(const std::string& name);

    KUZU_API static std::string getLocalPathForExtensionLib(main::ClientContext* context,
        const std::string& extensionName);

    KUZU_API static std::string getLocalPathForExtensionLoader(main::ClientContext* context,
        const std::string& extensionName);

    KUZU_API static std::string getLocalPathForExtensionInstaller(main::ClientContext* context,
        const std::string& extensionName);

    KUZU_API static std::string getLocalExtensionDir(main::ClientContext* context,
        const std::string& extensionName);

    KUZU_API static std::string appendLibSuffix(const std::string& libName);

    KUZU_API static std::string getLocalPathForSharedLib(main::ClientContext* context,
        const std::string& libName);

    KUZU_API static std::string getLocalPathForSharedLib(main::ClientContext* context);

    KUZU_API static void registerTableFunction(main::Database& database,
        std::unique_ptr<function::TableFunction> function);

    KUZU_API static void registerFunctionSet(main::Database& database, std::string name,
        function::function_set functionSet, catalog::CatalogEntryType functionType);

    static bool isOfficialExtension(const std::string& extension);
};

class KUZU_API ExtensionLibLoader {
public:
    static constexpr const char* EXTENSION_LOAD_FUNC_NAME = "load";

    static constexpr const char* EXTENSION_INIT_FUNC_NAME = "init";

    static constexpr const char* EXTENSION_INSTALL_FUNC_NAME = "install";

public:
    ExtensionLibLoader(const std::string& extensionName, const std::string& path);

    ext_load_func_t getLoadFunc();

    ext_init_func_t getInitFunc();

    ext_install_func_t getInstallFunc();

private:
    void* getDynamicLibFunc(const std::string& funcName);

private:
    std::string extensionName;
    void* libHdl;
};

struct ExtensionOptions {
    std::unordered_map<std::string, main::ExtensionOption> extensionOptions;

    void addExtensionOption(std::string name, common::LogicalTypeID type,
        common::Value defaultValue);

    main::ExtensionOption* getExtensionOption(std::string name);
};

#ifdef _WIN32
std::wstring utf8ToUnicode(const char* input);

void* dlopen(const char* file, int /*mode*/);

void* dlsym(void* handle, const char* name);
#endif

} // namespace extension
} // namespace kuzu
