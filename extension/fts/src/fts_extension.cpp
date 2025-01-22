#include "fts_extension.h"

#include "catalog/catalog.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/serializer/buffered_reader.h"
#include "function/create_fts_index.h"
#include "function/drop_fts_index.h"
#include "function/query_fts_index.h"
#include "function/stem.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace fts_extension {

using namespace extension;

static void initFTSEntries(const transaction::Transaction* transaction, catalog::Catalog& catalog) {
    for (auto& indexEntry : catalog.getIndexEntries(transaction)) {
        if (indexEntry->getIndexType() == FTSIndexCatalogEntry::TYPE_NAME) {
            indexEntry->setAuxInfo(FTSIndexAuxInfo::deserialize(indexEntry->getAuxBufferReader()));
        }
    }
}

void FTSExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ExtensionUtils::addScalarFunc<StemFunction>(db);
    ExtensionUtils::addGDSFunc<QueryFTSFunction>(db);
    ExtensionUtils::addStandaloneTableFunc<CreateFTSFunction>(db);
    ExtensionUtils::addStandaloneTableFunc<DropFTSFunction>(db);
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

INIT_EXPORT const char* name() {
    return kuzu::fts_extension::FTSExtension::EXTENSION_NAME;
}
}
