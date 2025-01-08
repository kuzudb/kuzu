#pragma once

#include <string>

namespace kuzu {
namespace extension {

enum class ExtensionSource : uint8_t { OFFICIAL, USER };

class LoadedExtension {

public:
    LoadedExtension(std::string extensionName, std::string fullPath, ExtensionSource source)
        : extensionName{std::move(extensionName)}, fullPath{std::move(fullPath)}, source{source} {}

    std::string getExtensionName() const { return extensionName; }

    std::string toCypher();

private:
    std::string extensionName;
    std::string fullPath;
    ExtensionSource source;
};

} // namespace extension
} // namespace kuzu
