#include "common/constants.h"
#include "main/database_instance.h"

namespace kuzu {
namespace main {

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
void DatabaseInstance::invalidateCache() {
    if (dbType != common::ATTACHED_KUZU_DB_TYPE) {
        auto catalogExtension = catalog->ptrCast<extension::CatalogExtension>();
        catalogExtension->invalidateCache();
    }
}

} // namespace main
} // namespace kuzu
