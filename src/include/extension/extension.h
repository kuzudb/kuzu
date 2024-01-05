#pragma once

#include <string>

#include "common/api.h"

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

    static std::string getExtensionDir(main::Database* database);

    static std::string getExtensionPath(const std::string& extensionDir, const std::string& name);

    static bool isFullPath(const std::string& extension);

    static ExtensionRepoInfo getExtensionRepoInfo(const std::string& extension);
};

} // namespace extension
} // namespace kuzu
