#pragma once

#include "binder/expression/expression.h"
#include "common/vector/value_vector.h"
#include "graph/graph.h"
#include "graph/graph_entry.h"

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
struct ExecutionContext;
} // namespace processor

namespace function {

class ParallelUtils;

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

    virtual std::vector<common::ValueVector*>& getOutputVectors() { return outputVectors; }

    virtual void init(main::ClientContext* clientContext) = 0;

    virtual inline uint64_t getWork() { return UINT64_MAX; }

    virtual std::unique_ptr<GDSLocalState> copy() = 0;

public:
    std::vector<common::ValueVector*> outputVectors;
    std::unique_ptr<graph::NbrScanState> nbrScanState;
};

// Base class for every graph data science algorithm.
class GDSAlgorithm {
public:
    GDSAlgorithm() = default;
    GDSAlgorithm(const GDSAlgorithm& other) {
        parallelUtils = other.parallelUtils;
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

    inline void setParallelUtils(std::shared_ptr<ParallelUtils>& parallelUtils_) {
        parallelUtils = parallelUtils_;
    }

    virtual void exec(processor::ExecutionContext* executionContext) = 0;

    inline GDSLocalState* getGDSLocalState() { return localState.get(); }

    // TODO: We should get rid of this copy interface (e.g. using stateless design) or at least make
    // sure the fields that cannot be copied, such as graph or factorized table and localState, are
    // wrapped in a different class.
    virtual std::unique_ptr<GDSAlgorithm> copy() const = 0;

protected:
    virtual void initLocalState(main::ClientContext* context) = 0;

protected:
    std::unique_ptr<GDSBindData> bindData;
    std::shared_ptr<ParallelUtils> parallelUtils;
    processor::FactorizedTable* table;
    processor::GDSCallSharedState* sharedState;
    std::unique_ptr<GDSLocalState> localState;
};

} // namespace function
} // namespace kuzu
