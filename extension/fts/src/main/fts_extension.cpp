#include "main/fts_extension.h"

#include "catalog/catalog.h"
#include "catalog/fts_index_catalog_entry.h"
#include "function/create_fts_index.h"
#include "function/drop_fts_index.h"
#include "function/query_fts_index.h"
#include "function/stem.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

using namespace extension;

static void initFTSEntries(const transaction::Transaction* transaction, catalog::Catalog& catalog) {
    for (auto& indexEntry : catalog.getIndexEntries(transaction)) {
        if (indexEntry->getIndexType() == FTSIndexCatalogEntry::TYPE_NAME &&
            !indexEntry->isLoaded()) {
            indexEntry->setAuxInfo(FTSIndexAuxInfo::deserialize(indexEntry->getAuxBufferReader()));
        }
    }
}

void FtsExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ExtensionUtils::addScalarFunc<StemFunction>(db);
    ExtensionUtils::addTableFunc<QueryFTSFunction>(db);
    ExtensionUtils::addStandaloneTableFunc<CreateFTSFunction>(db);
    ExtensionUtils::addInternalStandaloneTableFunc<InternalCreateFTSFunction>(db);
    ExtensionUtils::addStandaloneTableFunc<DropFTSFunction>(db);
    ExtensionUtils::addInternalStandaloneTableFunc<InternalDropFTSFunction>(db);
    initFTSEntries(&transaction::DUMMY_TRANSACTION, *db.getCatalog());
}

} // namespace fts_extension
} // namespace kuzu

#if defined(BUILD_DYNAMIC_LOAD)
extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::fts_extension::FtsExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::fts_extension::FtsExtension::EXTENSION_NAME;
}
}
#endif
