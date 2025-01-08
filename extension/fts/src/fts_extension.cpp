#include "fts_extension.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "catalog/fts_index_catalog_entry.h"
#include "function/create_fts_index.h"
#include "function/drop_fts_index.h"
#include "function/query_fts_index.h"
#include "function/stem.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace fts_extension {

static void initFTSEntries(transaction::Transaction* tx, catalog::Catalog& catalog) {
    auto indexEntries = catalog.getIndexes()->getEntries(tx);
    for (auto& [_, entry] : indexEntries) {
        auto indexEntry = entry->ptrCast<catalog::IndexCatalogEntry>();
        if (indexEntry->getIndexType() == FTSIndexCatalogEntry::TYPE_NAME) {
            auto deserializedEntry = FTSIndexCatalogEntry::deserializeAuxInfo(indexEntry);
            catalog.dropIndex(tx, indexEntry->getTableID(), indexEntry->getIndexName());
            catalog.createIndex(tx, std::move(deserializedEntry));
        }
    }
}

void FTSExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ADD_SCALAR_FUNC(StemFunction);
    ADD_GDS_FUNC(QueryFTSFunction);
    db.addStandaloneCallFunction(CreateFTSFunction::name, CreateFTSFunction::getFunctionSet());
    db.addStandaloneCallFunction(DropFTSFunction::name, DropFTSFunction::getFunctionSet());
    initFTSEntries(context->getTransaction(), *db.getCatalog());
}

} // namespace fts_extension
} // namespace kuzu

extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::fts_extension::FTSExtension::load(context);
}
}
