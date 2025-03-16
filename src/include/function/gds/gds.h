#pragma once

#include "common/mask.h"
#include "function/table/bind_data.h"
#include "graph/graph.h"
#include "graph/graph_entry.h"
#include "processor/result/factorized_table_pool.h"

namespace kuzu {

namespace main {
class ClientContext;
}

namespace function {

struct KUZU_API GDSBindData : public TableFuncBindData {
    graph::GraphEntry graphEntry;
    std::shared_ptr<binder::Expression> nodeOutput;

    GDSBindData(binder::expression_vector columns, graph::GraphEntry graphEntry,
        std::shared_ptr<binder::Expression> nodeOutput)
        : TableFuncBindData{std::move(columns)}, graphEntry{graphEntry.copy()},
          nodeOutput{std::move(nodeOutput)} {}
    GDSBindData(const GDSBindData& other)
        : TableFuncBindData{other}, graphEntry{other.graphEntry.copy()},
          nodeOutput{other.nodeOutput}, resultTable{other.resultTable} {}

    void setResultFTable(std::shared_ptr<processor::FactorizedTable> table) {
        resultTable = std::move(table);
    }
    std::shared_ptr<processor::FactorizedTable> getResultTable() const { return resultTable; }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<GDSBindData>(*this);
    }

private:
    std::shared_ptr<processor::FactorizedTable> resultTable;
};

struct KUZU_API GDSFuncSharedState : public TableFuncSharedState {
    std::unique_ptr<graph::Graph> graph;

    GDSFuncSharedState(std::shared_ptr<processor::FactorizedTable> fTable,
        std::unique_ptr<graph::Graph> graph)
        : TableFuncSharedState{}, graph{std::move(graph)}, factorizedTablePool{std::move(fTable)} {}

    void setGraphNodeMask(std::unique_ptr<common::NodeOffsetMaskMap> maskMap);
    common::NodeOffsetMaskMap* getGraphNodeMaskMap() const { return graphNodeMask.get(); }

public:
    processor::FactorizedTablePool factorizedTablePool;

private:
    std::unique_ptr<common::NodeOffsetMaskMap> graphNodeMask = nullptr;
};

// Base class for every graph data science algorithm.
class KUZU_API GDSFunction {
    static constexpr char NODE_COLUMN_NAME[] = "node";

public:
    static graph::GraphEntry bindGraphEntry(main::ClientContext& context, const std::string& name);
    static std::shared_ptr<binder::Expression> bindNodeOutput(const TableFuncBindInput& bindInput,
        const std::vector<catalog::TableCatalogEntry*>& nodeEntries);
    static std::string bindColumnName(const parser::YieldVariable& yieldVariable,
        std::string expressionName);

    static std::unique_ptr<TableFuncSharedState> initSharedState(
        const TableFuncInitSharedStateInput& input);
    static void getLogicalPlan(planner::Planner* planner,
        const binder::BoundReadingClause& readingClause,
        std::shared_ptr<planner::LogicalOperator> logicalOp,
        const std::vector<std::unique_ptr<planner::LogicalPlan>>& logicalPlans);
    static std::unique_ptr<processor::PhysicalOperator> getPhysicalPlan(
        processor::PlanMapper* planMapper, const planner::LogicalOperator* logicalOp);
};

} // namespace function
} // namespace kuzu
