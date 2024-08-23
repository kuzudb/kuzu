#pragma once

#include <string>

#include "common/api.h"
#include "extension.h"

namespace kuzu {
namespace main {
class ClientContext;
}
namespace extension {

class KUZU_API ExtensionInstaller {
public:
    explicit ExtensionInstaller(const std::string extensionName)
        : extensionName{std::move(extensionName)} {}

    virtual ~ExtensionInstaller() = default;

    virtual void install(main::ClientContext* context);

protected:
    void tryDownloadExtensionFile(main::ClientContext* context, const ExtensionRepoInfo& info,
        const std::string& localFilePath);

protected:
    std::string extensionName;
};

} // namespace extension
} // namespace kuzu
