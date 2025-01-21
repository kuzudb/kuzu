#pragma once

#include <string>

#include "catalog/catalog.h"
#include "catalog/catalog_entry/catalog_entry_type.h"
#include "common/api.h"
#include "function/function.h"
#include "main/database.h"
#include "main/db_config.h"
#include "transaction/transaction.h"

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
typedef const char* (*ext_name_func_t)();
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

enum class ExtensionSource : uint8_t { OFFICIAL, USER };

struct ExtensionSourceUtils {
    static std::string toString(ExtensionSource source);
};

template<typename T>
void addFunc(main::Database& database, std::string name, catalog::CatalogEntryType functionType) {
    auto catalog = database.getCatalog();
    if (catalog->containsFunction(&transaction::DUMMY_TRANSACTION, name)) {
        return;
    }
    catalog->addFunction(&transaction::DUMMY_TRANSACTION, functionType, std::move(name),
        T::getFunctionSet());
}

struct KUZU_API ExtensionUtils {
    static constexpr const char* EXTENSION_FILE_REPO = "http://extension.kuzudb.com/v{}/{}/{}/{}";

    static constexpr const char* SHARED_LIB_REPO = "http://extension.kuzudb.com/v{}/{}/common/{}";

    static constexpr const char* EXTENSION_FILE_NAME = "lib{}.kuzu_extension";

    static constexpr const char* OFFICIAL_EXTENSION[] = {"HTTPFS", "POSTGRES", "DUCKDB", "JSON",
        "SQLITE", "FTS", "DELTA", "ICEBERG"};

    static constexpr const char* EXTENSION_LOADER_SUFFIX = "_loader";

    static constexpr const char* EXTENSION_INSTALLER_SUFFIX = "_installer";

    static bool isFullPath(const std::string& extension);

    static ExtensionRepoInfo getExtensionLibRepoInfo(const std::string& extensionName);

    static ExtensionRepoInfo getExtensionLoaderRepoInfo(const std::string& extensionName);

    static ExtensionRepoInfo getExtensionInstallerRepoInfo(const std::string& extensionName);

    static ExtensionRepoInfo getSharedLibRepoInfo(const std::string& fileName);

    static std::string getExtensionFileName(const std::string& name);

    static std::string getLocalPathForExtensionLib(main::ClientContext* context,
        const std::string& extensionName);

    static std::string getLocalPathForExtensionLoader(main::ClientContext* context,
        const std::string& extensionName);

    static std::string getLocalPathForExtensionInstaller(main::ClientContext* context,
        const std::string& extensionName);

    static std::string getLocalExtensionDir(main::ClientContext* context,
        const std::string& extensionName);

    static std::string appendLibSuffix(const std::string& libName);

    static std::string getLocalPathForSharedLib(main::ClientContext* context,
        const std::string& libName);

    static std::string getLocalPathForSharedLib(main::ClientContext* context);

    static bool isOfficialExtension(const std::string& extension);

    template<typename T>
    static void addTableFunc(main::Database& database) {
        addFunc<T>(database, T::name, catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
    }

    template<typename T>
    static void addStandaloneTableFunc(main::Database& database) {
        addFunc<T>(database, T::name, catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY);
    }

    template<typename T>
    static void addScalarFunc(main::Database& database) {
        addFunc<T>(database, T::name, catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
    }

    template<typename T>
    static void addScalarFuncAlias(main::Database& database) {
        addFunc<typename T::alias>(database, T::name,
            catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
    }

    template<typename T>
    static void addGDSFunc(main::Database& database) {
        addFunc<T>(database, T::name, catalog::CatalogEntryType::GDS_FUNCTION_ENTRY);
    }
};

class KUZU_API ExtensionLibLoader {
public:
    static constexpr const char* EXTENSION_LOAD_FUNC_NAME = "load";

    static constexpr const char* EXTENSION_INIT_FUNC_NAME = "init";

    static constexpr const char* EXTENSION_NAME_FUNC_NAME = "name";

    static constexpr const char* EXTENSION_INSTALL_FUNC_NAME = "install";

public:
    ExtensionLibLoader(const std::string& extensionName, const std::string& path);

    ext_load_func_t getLoadFunc();

    ext_init_func_t getInitFunc();

    ext_name_func_t getNameFunc();

    ext_install_func_t getInstallFunc();

private:
    void* getDynamicLibFunc(const std::string& funcName);

private:
    std::string extensionName;
    void* libHdl;
};

#ifdef _WIN32
std::wstring utf8ToUnicode(const char* input);

void* dlopen(const char* file, int /*mode*/);

void* dlsym(void* handle, const char* name);
#endif

} // namespace extension
} // namespace kuzu
