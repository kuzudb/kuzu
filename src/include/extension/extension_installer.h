#pragma once

#include <string>

#include "common/api.h"
#include "extension.h"

namespace kuzu {
namespace main {
class ClientContext;
}
namespace extension {

struct InstallExtensionInfo {
    std::string name;
    std::string repo;
};

class KUZU_API ExtensionInstaller {
public:
    ExtensionInstaller(const InstallExtensionInfo& info, main::ClientContext& context)
        : info{info}, context{context} {}

    virtual ~ExtensionInstaller() = default;

    virtual void install();

protected:
    void tryDownloadExtensionFile(const ExtensionRepoInfo& info, const std::string& localFilePath);

private:
    void installExtension();
    void installDependencies();

protected:
    const InstallExtensionInfo& info;
    main::ClientContext& context;
};

} // namespace extension
} // namespace kuzu
