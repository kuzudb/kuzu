#pragma once

#include "loaded_extension.h"
#include "main/client_context.h"

namespace kuzu {
namespace extension {

class KUZU_API ExtensionManager {
public:
    void loadExtension(const std::string& path, main::ClientContext* context);

    std::string toCypher();

private:
    std::vector<LoadedExtension> loadedExtensions;
};

} // namespace extension
} // namespace kuzu
