#include "function/query_fts_bind_data.h"

#include "binder/expression/expression_util.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"
#include "function/stem.h"
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
            k = optionalParam;
        } else if (paramName == B::NAME) {
            b = optionalParam;
        } else if (paramName == Conjunctive::NAME) {
            conjunctive = optionalParam;
        } else {
            throw common::BinderException{"Unknown optional parameter: " + paramName};
        }
    }
}

QueryFTSConfig QueryFTSOptionalParams::getConfig() const {
    QueryFTSConfig config;
    if (k != nullptr) {
        config.k = ExpressionUtil::evaluateLiteral<double>(*k, LogicalType::DOUBLE(), K::validate);
    }
    if (b != nullptr) {
        config.b = ExpressionUtil::evaluateLiteral<double>(*b, LogicalType::DOUBLE(), B::validate);
    }
    if (conjunctive != nullptr) {
        config.isConjunctive =
            ExpressionUtil::evaluateLiteral<bool>(*conjunctive, LogicalType::BOOL());
    }
    return config;
}

std::vector<std::string> QueryFTSBindData::getTerms(main::ClientContext& context) const {
    auto queryInStr = ExpressionUtil::evaluateLiteral<std::string>(*query, LogicalType::STRING());
    FTSUtils::normalizeQuery(queryInStr);
    auto terms = StringUtils::split(queryInStr, " ");
    auto config = entry.getAuxInfo().cast<FTSIndexAuxInfo>().config;
    auto stopWordsTable = context.getStorageManager()
                              ->getTable(context.getCatalog()
                                             ->getTableCatalogEntry(context.getTransaction(),
                                                 config.stopWordsTableName)
                                             ->getTableID())
                              ->ptrCast<NodeTable>();
    return FTSUtils::stemTerms(terms, entry.getAuxInfo().cast<FTSIndexAuxInfo>().config,
        context.getMemoryManager(), stopWordsTable, context.getTransaction(),
        getConfig().isConjunctive);
}

} // namespace fts_extension
} // namespace kuzu
