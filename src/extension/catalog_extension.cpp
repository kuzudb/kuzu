#include "extension/catalog_extension.h"

namespace kuzu {
namespace extension {

void CatalogExtension::invalidateCache() {
    tables = std::make_unique<catalog::CatalogSet>();
    init(nullptr);
}

} // namespace extension
} // namespace kuzu
