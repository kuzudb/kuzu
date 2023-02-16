#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {


class FactorizationRewriter {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
//    void preAppendFlatten(planner::LogicalOperator* op, );

    void visitOperator(planner::LogicalOperator* op);
    void visitExtend(planner::LogicalOperator* op);
};

}
}