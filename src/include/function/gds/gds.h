#pragma once

#include "binder/expression/expression.h"
#include "graph/graph_entry.h"
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

// Struct maintaining GDS specific information that needs to be obtained at compile time.
struct GDSBindData {
    std::shared_ptr<binder::Expression> nodeOutput;

    explicit GDSBindData(std::shared_ptr<binder::Expression> nodeOutput)
        : nodeOutput{std::move(nodeOutput)} {}
    GDSBindData(const GDSBindData& other) : nodeOutput{other.nodeOutput} {}

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
class GDSAlgorithm {
protected:
    static constexpr char NODE_COLUMN_NAME[] = "_node";

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

    virtual binder::expression_vector getResultColumns(binder::Binder* binder) const = 0;

    virtual void bind(const binder::expression_vector& params, binder::Binder* binder,
        graph::GraphEntry& graphEntry) = 0;
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
    // TODO(Semih/Xiyang): See if this will be still needed after PageRank and other algorithms are
    // updated. GDSAlgorithms implementing recursive joins do not use this function.
    virtual void initLocalState(main::ClientContext*) {}

protected:
    std::shared_ptr<binder::Expression> bindNodeOutput(binder::Binder* binder,
        const graph::GraphEntry& graphEntry);

protected:
    std::unique_ptr<GDSBindData> bindData;
    std::shared_ptr<processor::GDSCallSharedState> sharedState;
};

} // namespace function
} // namespace kuzu
