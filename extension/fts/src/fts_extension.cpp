#include "fts_extension.h"

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "function/create_fts_index.h"
#include "function/drop_fts_index.h"
#include "function/get_keys.h"
#include "function/query_fts_gds.h"
#include "function/query_fts_index.h"
#include "function/stem.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace fts_extension {

void FTSExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ADD_SCALAR_FUNC(StemFunction);
    ADD_SCALAR_FUNC(GetKeysFunction);
    ADD_GDS_FUNC(QFTSFunction);
    db.addStandaloneCallFunction(CreateFTSFunction::name, CreateFTSFunction::getFunctionSet());
    db.addTableFunction(QueryFTSFunction::name, QueryFTSFunction::getFunctionSet());
    db.addStandaloneCallFunction(DropFTSFunction::name, DropFTSFunction::getFunctionSet());
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
