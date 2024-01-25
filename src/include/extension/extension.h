#pragma once

#include <string>
#include <unordered_map>

#include "common/api.h"
#include "main/db_config.h"

namespace kuzu {
namespace main {
class Database;
} // namespace main

namespace extension {

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
    static constexpr const char* EXTENSION_REPO =
        "http://extension.kuzudb.com/v{}/{}/lib{}.kuzu_extension";

    static std::string getExtensionPath(const std::string& extensionDir, const std::string& name);

    static bool isFullPath(const std::string& extension);

    static ExtensionRepoInfo getExtensionRepoInfo(const std::string& extension);
};

struct ExtensionOptions {
    std::unordered_map<std::string, main::ExtensionOption> extensionOptions;

    void addExtensionOption(
        std::string name, common::LogicalTypeID type, common::Value defaultValue);

    main::ExtensionOption* getExtensionOption(std::string name);

    std::unordered_map<std::string, main::ExtensionOption>& getExtensionOptions();
};

} // namespace extension
} // namespace kuzu
