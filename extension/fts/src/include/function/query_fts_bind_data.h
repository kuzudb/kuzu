#pragma once

#include "binder/expression/node_expression.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "function/fts_config.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace fts_extension {

struct QueryFTSBindData final : function::GDSBindData {
    std::shared_ptr<binder::Expression> query;
    const catalog::IndexCatalogEntry& entry;
    QueryFTSConfig config;
    common::table_id_t outputTableID;

    QueryFTSBindData(graph::GraphEntry graphEntry, std::shared_ptr<binder::Expression> docs,
        std::shared_ptr<binder::Expression> query, const catalog::IndexCatalogEntry& entry,
        QueryFTSConfig config)
        : GDSBindData{std::move(graphEntry), std::move(docs)}, query{std::move(query)},
          entry{entry}, config{config},
          outputTableID{
              nodeOutput->constCast<binder::NodeExpression>().getSingleEntry()->getTableID()} {}
    QueryFTSBindData(const QueryFTSBindData& other)
        : GDSBindData{other}, query{other.query}, entry{other.entry}, config{other.config},
          outputTableID{other.outputTableID} {}

    bool hasNodeInput() const override { return false; }

    std::vector<std::string> getTerms(main::ClientContext& context) const;

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

} // namespace fts_extension
} // namespace kuzu
