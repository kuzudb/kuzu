#pragma once

#include "binder/expression/node_expression.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "function/fts_config.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace fts_extension {

struct QueryFTSOptionalParams {
    std::shared_ptr<binder::Expression> k;
    std::shared_ptr<binder::Expression> b;
    std::shared_ptr<binder::Expression> conjunctive;

    explicit QueryFTSOptionalParams(const binder::expression_vector& optionalParams);

    QueryFTSConfig getConfig() const;
};

struct QueryFTSBindData final : function::GDSBindData {
    std::shared_ptr<binder::Expression> query;
    const catalog::IndexCatalogEntry& entry;
    QueryFTSOptionalParams optionalParams;
    common::table_id_t outputTableID;

    QueryFTSBindData(binder::expression_vector columns, graph::GraphEntry graphEntry,
        std::shared_ptr<binder::Expression> docs, std::shared_ptr<binder::Expression> query,
        const catalog::IndexCatalogEntry& entry, QueryFTSOptionalParams optionalParams)
        : GDSBindData{std::move(columns), std::move(graphEntry), std::move(docs)},
          query{std::move(query)}, entry{entry}, optionalParams{std::move(optionalParams)},
          outputTableID{
              nodeOutput->constCast<binder::NodeExpression>().getSingleEntry()->getTableID()} {}
    QueryFTSBindData(const QueryFTSBindData& other)
        : GDSBindData{other}, query{other.query}, entry{other.entry},
          optionalParams{other.optionalParams}, outputTableID{other.outputTableID} {}

    std::vector<std::string> getTerms(main::ClientContext& context) const;

    QueryFTSConfig getConfig() const { return optionalParams.getConfig(); }

    std::unique_ptr<function::TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

} // namespace fts_extension
} // namespace kuzu
