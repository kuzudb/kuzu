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
    std::shared_ptr<binder::Expression> topK;

    QueryFTSOptionalParams(main::ClientContext* context,
        const binder::expression_vector& optionalParams);

    QueryFTSConfig getConfig() const;
};

struct QueryFTSBindData final : function::GDSBindData {
    const catalog::IndexCatalogEntry& entry;
    QueryFTSOptionalParams optionalParams;
    common::table_id_t outputTableID;
    common::idx_t numDocs;
    double avgDocLen;
    std::vector<std::string> queryTerms;

    QueryFTSBindData(binder::expression_vector columns, graph::NativeGraphEntry graphEntry,
        std::shared_ptr<binder::Expression> docs, const catalog::IndexCatalogEntry& entry,
        QueryFTSOptionalParams optionalParams, common::idx_t numDocs, double avgDocLen,
        std::vector<std::string> queryTerms)
        : GDSBindData{std::move(columns), std::move(graphEntry), std::move(docs)}, entry{entry},
          optionalParams{std::move(optionalParams)},
          outputTableID{nodeOutput->constCast<binder::NodeExpression>().getTableIDs()[0]},
          numDocs{numDocs}, avgDocLen{avgDocLen}, queryTerms{std::move(queryTerms)} {
        auto& nodeExpr = nodeOutput->constCast<binder::NodeExpression>();
        KU_ASSERT(nodeExpr.getNumEntries() == 1);
        outputTableID = nodeExpr.getEntry(0)->getTableID();
    }
    QueryFTSBindData(const QueryFTSBindData& other)
        : GDSBindData{other}, entry{other.entry}, optionalParams{other.optionalParams},
          outputTableID{other.outputTableID}, numDocs{other.numDocs}, avgDocLen{other.avgDocLen},
          queryTerms{std::vector(other.queryTerms)} {}

    QueryFTSConfig getConfig() const { return optionalParams.getConfig(); }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

} // namespace fts_extension
} // namespace kuzu
