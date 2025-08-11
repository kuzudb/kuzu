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
#if defined(KUZU_ENABLE_JIEBA)
#include "cppjieba/Jieba.hpp"
#endif

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
    std::vector<std::string> terms;
    {
        std::string s = queryInStr;
        if (common::StringUtils::getLower(config.tokenizer) == "jieba") {
#if defined(KUZU_ENABLE_JIEBA)
            cppjieba::Jieba jieba(config.jiebaDictDir + "/jieba.dict.utf8",
                config.jiebaDictDir + "/hmm_model.utf8",
                config.jiebaDictDir + "/user.dict.utf8",
                config.jiebaDictDir + "/idf.utf8",
                config.jiebaDictDir + "/stop_words.utf8");
            jieba.CutForSearch(s, terms);
#else
            RE2 pattern{config.ignorePatternQuery};
            RE2::GlobalReplace(&s, pattern, " ");
            StringUtils::toLower(s);
            terms = StringUtils::split(s, " ", true);
#endif
        } else {
            RE2 pattern{config.ignorePatternQuery};
            RE2::GlobalReplace(&s, pattern, " ");
            StringUtils::toLower(s);
            terms = StringUtils::split(s, " ", true);
        }
    }
    auto stopWordsTable = context.getStorageManager()
                              ->getTable(context.getCatalog()
                                             ->getTableCatalogEntry(context.getTransaction(),
                                                 config.stopWordsTableName)
                                             ->getTableID())
                              ->ptrCast<NodeTable>();
    return FTSUtils::stemTerms(terms, entry.getAuxInfo().cast<FTSIndexAuxInfo>().config,
        context.getMemoryManager(), stopWordsTable, context.getTransaction(),
        optionalParams->constCast<QueryFTSOptionalParams>().conjunctive.getParamVal(),
        true /* isQuery */);
}

} // namespace fts_extension
} // namespace kuzu
