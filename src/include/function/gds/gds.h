#pragma once

#include "binder/expression/expression.h"
#include "graph/graph_entry.h"
#include "parser/query/reading_clause/yield_variable.h"
#include "processor/operator/gds_call_shared_state.h"

namespace kuzu {
namespace binder {
class Binder;
}
namespace main {
class ClientContext;
}
namespace processor {
class FactorizedTable;
struct ExecutionContext;
} // namespace processor

namespace function {

class PathLengths;

struct GDSBindInput {
    binder::expression_vector params;
    binder::expression_vector optionalParams;
    binder::Binder* binder = nullptr;
    std::vector<parser::YieldVariable> yieldVariables;

    GDSBindInput() = default;

    std::shared_ptr<binder::Expression> getParam(common::idx_t idx) const { return params[idx]; }
    std::shared_ptr<binder::Expression> getOptionalParam(common::idx_t idx) const {
        return optionalParams[idx];
    }
    common::idx_t getNumParams() const { return params.size(); }
};

struct GDSBindData {
    graph::GraphEntry graphEntry;
    std::shared_ptr<binder::Expression> nodeOutput;

    GDSBindData(graph::GraphEntry graphEntry, std::shared_ptr<binder::Expression> nodeOutput)
        : graphEntry{graphEntry.copy()}, nodeOutput{std::move(nodeOutput)} {}
    GDSBindData(const GDSBindData& other)
        : graphEntry{other.graphEntry.copy()}, nodeOutput{other.nodeOutput} {}

    virtual ~GDSBindData() = default;

    virtual bool hasNodeInput() const { return false; }

    virtual std::shared_ptr<binder::Expression> getNodeInput() const { return nullptr; }

    virtual bool hasNodeOutput() const { return false; }

    virtual std::shared_ptr<binder::Expression> getNodeOutput() const {
        KU_ASSERT(nodeOutput != nullptr);
        return nodeOutput;
    }

    virtual std::unique_ptr<GDSBindData> copy() const {
        return std::make_unique<GDSBindData>(*this);
    }

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

// Base class for every graph data science algorithm.
class KUZU_API GDSAlgorithm {
protected:
    static constexpr char NODE_COLUMN_NAME[] = "node";

public:
    GDSAlgorithm() = default;

    GDSAlgorithm(const GDSAlgorithm& other) {
        if (other.bindData != nullptr) {
            bindData = other.bindData->copy();
        }
        sharedState = other.sharedState;
    }

    virtual ~GDSAlgorithm() = default;

    virtual std::vector<common::LogicalTypeID> getParameterTypeIDs() const { return {}; }

    virtual binder::expression_vector getResultColumns(
        const function::GDSBindInput& bindInput) const = 0;

    virtual void bind(const GDSBindInput& input, main::ClientContext& context) = 0;
    // When compiling recursive pattern (e.g. [e*1..2]) as GDS.
    // We skip binding and directly set bind data.
    void setBindData(std::unique_ptr<GDSBindData> bindData_) { bindData = std::move(bindData_); }

    const GDSBindData* getBindData() const { return bindData.get(); }

    // Note: The reason this field is set separately here and not inside constructor is that the
    // original GDSAlgorithm is constructed in the binding stage. In contrast, sharedState is
    // constructed during mapping phase. Also as a coding pattern, "shared state objects" used
    // inside physical operators are set inside the init functions inside the operators. Although
    // GDSAlgorithm is not an operator, it is close to an operator. It is simply used by the GDSCall
    // operator, which is basically a wrapper around a GDSAlgorithm.
    void setSharedState(std::shared_ptr<processor::GDSCallSharedState> _sharedState) {
        sharedState = _sharedState;
    }

    virtual void exec(processor::ExecutionContext* executionContext) = 0;

    virtual std::unique_ptr<GDSAlgorithm> copy() const = 0;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }

protected:
    graph::GraphEntry bindGraphEntry(main::ClientContext& context, const std::string& name);
    std::shared_ptr<binder::Expression> bindNodeOutput(const GDSBindInput& bindInput,
        const std::vector<catalog::TableCatalogEntry*>& nodeEntries);

    static std::string bindColumnName(const parser::YieldVariable& yieldVariable,
        std::string expressionName);

protected:
    std::unique_ptr<GDSBindData> bindData;
    std::shared_ptr<processor::GDSCallSharedState> sharedState;
};

} // namespace function
} // namespace kuzu
