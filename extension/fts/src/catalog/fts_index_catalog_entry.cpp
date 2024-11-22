#include "catalog/fts_index_catalog_entry.h"

namespace kuzu {
namespace fts_extension {

std::unique_ptr<catalog::IndexCatalogEntry> FTSIndexCatalogEntry::copy() const {
    auto other = std::make_unique<FTSIndexCatalogEntry>();
    other->numDocs = numDocs;
    other->avgDocLen = avgDocLen;
    other->copyFrom(*this);
    return other;
}

void FTSIndexCatalogEntry::canDropProperty() const {

}

} // namespace fts_extension
} // namespace kuzu
