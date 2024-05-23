#pragma once

#include "binder/expression/expression.h"
#include "graph/graph.h"

namespace kuzu {
namespace main {
class ClientContext;
}
namespace processor {
class FactorizedTable;
}
namespace function {

// Struct maintaining GDS specific information that needs to be obtained at compile time.
struct GDSBindData {
    virtual ~GDSBindData() = default;

    virtual std::unique_ptr<GDSBindData> copy() const = 0;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<GDSBindData*, TARGET*>(this);
    }
};

class GDSLocalState {
public:
    virtual ~GDSLocalState() = default;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<GDSLocalState*, TARGET*>(this);
    }
};

// Base class for every graph data science algorithm.
class GDSAlgorithm {
public:
    GDSAlgorithm() = default;
    GDSAlgorithm(const GDSAlgorithm& other) {
        if (other.bindData != nullptr) {
            bindData = other.bindData->copy();
        }
    }
    virtual ~GDSAlgorithm() = default;

    virtual std::vector<common::LogicalTypeID> getParameterTypeIDs() const { return {}; }
    virtual std::vector<std::string> getResultColumnNames() const = 0;
    virtual std::vector<common::LogicalType> getResultColumnTypes() const = 0;

    virtual void bind(const binder::expression_vector&) { bindData = nullptr; }

    void init(graph::Graph* graph_, processor::FactorizedTable* table_,
        main::ClientContext* context) {
        graph = graph_;
        table = table_;
        initLocalState(context);
    }

    virtual void exec() = 0;

    // TODO: We should get rid of this copy interface (e.g. using stateless design) or at least make
    // sure the fields that cannot be copied, such as graph or factorized table and localState, are
    // wrapped in a different class.
    virtual std::unique_ptr<GDSAlgorithm> copy() const = 0;

protected:
    virtual void initLocalState(main::ClientContext* context) = 0;

protected:
    std::unique_ptr<GDSBindData> bindData;
    graph::Graph* graph;
    processor::FactorizedTable* table;
    std::unique_ptr<GDSLocalState> localState;
};

} // namespace function
} // namespace kuzu
