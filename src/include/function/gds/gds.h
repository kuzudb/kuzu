#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {
class Binder;
}
namespace main {
class ClientContext;
}
namespace processor {
struct GDSCallSharedState;
class FactorizedTable;
} // namespace processor
namespace function {

// Struct maintaining GDS specific information that needs to be obtained at compile time.
struct GDSBindData {
    std::shared_ptr<binder::Expression> nodeInput = nullptr;

    GDSBindData() = default;
    explicit GDSBindData(std::shared_ptr<binder::Expression> nodeInput)
        : nodeInput{std::move(nodeInput)} {}
    GDSBindData(const GDSBindData& other) : nodeInput{other.nodeInput} {}
    virtual ~GDSBindData() = default;

    bool hasNodeInput() const { return nodeInput != nullptr; }

    virtual std::unique_ptr<GDSBindData> copy() const {
        return std::make_unique<GDSBindData>(*this);
    }

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
    virtual binder::expression_vector getResultColumns(binder::Binder* binder) const = 0;

    virtual void bind(const binder::expression_vector&) {
        bindData = std::make_unique<GDSBindData>();
    }
    GDSBindData* getBindData() const { return bindData.get(); }

    void init(processor::GDSCallSharedState* sharedState, main::ClientContext* context);

    virtual void exec() = 0;

    // TODO: We should get rid of this copy interface (e.g. using stateless design) or at least make
    // sure the fields that cannot be copied, such as graph or factorized table and localState, are
    // wrapped in a different class.
    virtual std::unique_ptr<GDSAlgorithm> copy() const = 0;

protected:
    virtual void initLocalState(main::ClientContext* context) = 0;

protected:
    std::unique_ptr<GDSBindData> bindData;
    processor::GDSCallSharedState* sharedState;
    std::unique_ptr<GDSLocalState> localState;
};

} // namespace function
} // namespace kuzu
