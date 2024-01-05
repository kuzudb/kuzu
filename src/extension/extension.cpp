#include "extension/extension.h"

#include "common/string_format.h"
#include "common/string_utils.h"
#include "main/database.h"

namespace kuzu {
namespace extension {

std::string getOS() {
    std::string os = "linux";
#ifdef _WIN32
    os = "windows";
#elif defined(__APPLE__)
    os = "osx";
#endif
    return os;
}

std::string getArch() {
    std::string arch = "amd64";
#if defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
    arch = "arm64";
#endif
    return arch;
}

std::string getPlatform() {
    return getOS() + "_" + getArch();
}

std::string ExtensionUtils::getExtensionDir(kuzu::main::Database* database) {
    return common::stringFormat("{}/extension", database->databasePath);
}

std::string ExtensionUtils::getExtensionPath(
    const std::string& extensionDir, const std::string& name) {
    return common::stringFormat("{}/lib{}.kuzu_extension", extensionDir, name);
}

bool ExtensionUtils::isFullPath(const std::string& extension) {
    return extension.find('.') != std::string::npos || extension.find('/') != std::string::npos ||
           extension.find('\\') != std::string::npos;
}

ExtensionRepoInfo ExtensionUtils::getExtensionRepoInfo(const std::string& extension) {
    auto extensionURL =
        common::stringFormat(EXTENSION_REPO, KUZU_RELEASE_VERSION, getPlatform(), extension);
    common::StringUtils::replaceAll(extensionURL, "http://", "");
    auto hostNamePos = extensionURL.find('/');
    auto hostName = extensionURL.substr(0, hostNamePos);
    auto hostURL = "http://" + hostName;
    auto hostPath = extensionURL.substr(hostNamePos);
    return {hostPath, hostURL, extensionURL};
}

} // namespace extension
} // namespace kuzu
