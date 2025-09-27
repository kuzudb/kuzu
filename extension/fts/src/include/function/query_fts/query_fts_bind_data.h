#pragma once

#include "binder/expression/node_expression.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "function/fts_config.h"
#include "function/gds/gds.h"
#include "function/query_fts/query_fts_pattern_match.h"

namespace kuzu {
namespace fts_extension {

struct QueryFTSOptionalParams : public function::OptionalParams {
    function::OptionalParam<K> k;
    function::OptionalParam<B> b;
    function::OptionalParam<Conjunctive> conjunctive;
    function::OptionalParam<TopK> topK;

    explicit QueryFTSOptionalParams(const binder::expression_vector& optionalParams);

    // For copy only.
    QueryFTSOptionalParams(function::OptionalParam<K> k, function::OptionalParam<B> b,
        function::OptionalParam<Conjunctive> conjunctive, function::OptionalParam<TopK> topK)
        : k{std::move(k)}, b{std::move(b)}, conjunctive{std::move(conjunctive)},
          topK{std::move(topK)} {}

    void evaluateParams(main::ClientContext* context) override;

    std::unique_ptr<function::OptionalParams> copy() override {
        return std::make_unique<QueryFTSOptionalParams>(k, b, conjunctive, topK);
    }
};

struct QueryFTSBindData final : public function::GDSBindData {
    std::shared_ptr<binder::Expression> query;
    const catalog::IndexCatalogEntry& entry;
    common::table_id_t outputTableID;
    common::idx_t numDocs;
    double avgDocLen;
    pattern_match_algo patternMatchAlgo;

    QueryFTSBindData(binder::expression_vector columns, graph::NativeGraphEntry graphEntry,
        std::shared_ptr<binder::Expression> docs, std::shared_ptr<binder::Expression> query,
        const catalog::IndexCatalogEntry& entry,
        std::unique_ptr<QueryFTSOptionalParams> optionalParams, common::idx_t numDocs,
        double avgDocLen);

    QueryFTSBindData(const QueryFTSBindData& other)
        : GDSBindData{other}, query{other.query}, entry{other.entry},
          outputTableID{other.outputTableID}, numDocs{other.numDocs}, avgDocLen{other.avgDocLen},
          patternMatchAlgo{other.patternMatchAlgo} {}

    catalog::TableCatalogEntry* getTermsEntry(main::ClientContext& context) const;

    catalog::TableCatalogEntry* getOrigTermsEntry(main::ClientContext& context) const;

    std::vector<std::string> getQueryTerms(main::ClientContext& context) const;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

} // namespace fts_extension
} // namespace kuzu
