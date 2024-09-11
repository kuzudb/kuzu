#include "extension/catalog_extension.h"

namespace kuzu {
namespace extension {

void CatalogExtension::invalidateCache(transaction::Transaction* transaction) {
    tables = std::make_unique<catalog::CatalogSet>();
    init(transaction);
}

} // namespace extension
} // namespace kuzu
