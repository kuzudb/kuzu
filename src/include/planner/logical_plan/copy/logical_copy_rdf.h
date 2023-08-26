#pragma once

#include <memory>
#include <utility>

#include "binder/expression/expression.h"
#include "common/copier_config/copier_config.h"
#include "common/types/internal_id_t.h"
#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyRDF : public LogicalOperator {

public:
    LogicalCopyRDF(const common::CopyDescription& copyDescription,
        common::rdf_graph_id_t rdfGraphID, std::string rdfGraphName,
        std::shared_ptr<binder::Expression> subjectExpression,
        std::shared_ptr<binder::Expression> predicateExpression,
        std::shared_ptr<binder::Expression> objectExpression,
        std::shared_ptr<binder::Expression> offsetExpression,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{LogicalOperatorType::COPY_RDF}, copyDescription{copyDescription},
          rdfGraphID{rdfGraphID}, rdfGraphName{std::move(rdfGraphName)},
          subjectExpression{std::move(subjectExpression)},
          predicateExpression{std::move(predicateExpression)}, objectExpression{std::move(
                                                                   objectExpression)},
          offsetExpression{std::move(offsetExpression)}, outputExpression{
                                                             std::move(outputExpression)} {}

    inline std::string getExpressionsForPrinting() const override { return rdfGraphName; }

    inline common::CopyDescription getCopyDescription() const { return copyDescription; }

    inline common::table_id_t getRDFGraphID() const { return rdfGraphID; }

    inline std::shared_ptr<binder::Expression> getSubjectExpression() const {
        return subjectExpression;
    }

    inline std::shared_ptr<binder::Expression> getPredicateExpression() const {
        return predicateExpression;
    }

    inline std::shared_ptr<binder::Expression> getObjectExpression() const {
        return objectExpression;
    }

    inline std::shared_ptr<binder::Expression> getOffsetExpression() const {
        return offsetExpression;
    }

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyRDF>(copyDescription, rdfGraphID, rdfGraphName,
            subjectExpression, predicateExpression, objectExpression, offsetExpression,
            outputExpression);
    }

private:
    common::CopyDescription copyDescription;
    common::rdf_graph_id_t rdfGraphID;
    // Used for printing only.
    std::string rdfGraphName;
    std::shared_ptr<binder::Expression> subjectExpression;
    std::shared_ptr<binder::Expression> predicateExpression;
    std::shared_ptr<binder::Expression> objectExpression;
    std::shared_ptr<binder::Expression> offsetExpression;
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
