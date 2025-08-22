#include "function/query_fts_bind_data.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"
#include "libstemmer.h"
#include "re2.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;

QueryFTSOptionalParams::QueryFTSOptionalParams(const binder::expression_vector& optionalParams) {
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == K::NAME) {
            k = function::OptionalParam<K>(optionalParam);
        } else if (paramName == B::NAME) {
            b = function::OptionalParam<B>(optionalParam);
        } else if (paramName == Conjunctive::NAME) {
            conjunctive = function::OptionalParam<Conjunctive>(optionalParam);
        } else if (paramName == TopK::NAME) {
            topK = function::OptionalParam<TopK>(optionalParam);
        } else {
            throw common::BinderException{"Unknown optional parameter: " + paramName};
        }
    }
}

void QueryFTSOptionalParams::evaluateParams(main::ClientContext* context) {
    k.evaluateParam(context);
    b.evaluateParam(context);
    conjunctive.evaluateParam(context);
    topK.evaluateParam(context);
}

std::vector<std::string> QueryFTSBindData::getQueryTerms(main::ClientContext& context) const {
    auto queryInStr =
        ExpressionUtil::evaluateLiteral<std::string>(&context, query, LogicalType::STRING());
    auto config = entry.getAuxInfo().cast<FTSIndexAuxInfo>().config;
    FTSUtils::normalizeQuery(queryInStr, config.ignorePatternQuery);
    auto terms = FTSUtils::tokenizeString(queryInStr, config);
    auto stopWordsTable = StorageManager::Get(context)
                              ->getTable(catalog::Catalog::Get(context)
                                             ->getTableCatalogEntry(context.getTransaction(),
                                                 config.stopWordsTableName)
                                             ->getTableID())
                              ->ptrCast<NodeTable>();
    return FTSUtils::stemTerms(terms, entry.getAuxInfo().cast<FTSIndexAuxInfo>().config,
        MemoryManager::Get(context), stopWordsTable, context.getTransaction(),
        optionalParams->constCast<QueryFTSOptionalParams>().conjunctive.getParamVal(),
        true /* isQuery */);
}

} // namespace fts_extension
} // namespace kuzu
