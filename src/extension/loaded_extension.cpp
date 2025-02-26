#include "extension/loaded_extension.h"

#include "common/assert.h"

namespace kuzu {
namespace extension {

std::string LoadedExtension::toCypher() {
    switch (source) {
    case ExtensionSource::OFFICIAL:
        return common::stringFormat("INSTALL {};\nLOAD EXTENSION {};\n", extensionName,
            extensionName);
    case ExtensionSource::USER:
        return common::stringFormat("LOAD EXTENSION '{}';\n", fullPath);
    default:
        KU_UNREACHABLE;
    }
}

} // namespace extension
} // namespace kuzu
