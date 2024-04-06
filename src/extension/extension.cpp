#include "extension/extension.h"

#include "common/string_format.h"
#include "common/string_utils.h"

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

std::string ExtensionUtils::getExtensionPath(const std::string& extensionDir,
    const std::string& name) {
    return common::stringFormat("{}/lib{}.kuzu_extension", extensionDir, name);
}

bool ExtensionUtils::isFullPath(const std::string& extension) {
    return extension.find('.') != std::string::npos || extension.find('/') != std::string::npos ||
           extension.find('\\') != std::string::npos;
}

ExtensionRepoInfo ExtensionUtils::getExtensionRepoInfo(const std::string& extension) {
    auto extensionURL =
        common::stringFormat(EXTENSION_REPO, KUZU_EXTENSION_VERSION, getPlatform(), extension);
    common::StringUtils::replaceAll(extensionURL, "http://", "");
    auto hostNamePos = extensionURL.find('/');
    auto hostName = extensionURL.substr(0, hostNamePos);
    auto hostURL = "http://" + hostName;
    auto hostPath = extensionURL.substr(hostNamePos);
    return {hostPath, hostURL, extensionURL};
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

} // namespace extension
} // namespace kuzu
