#include <memory>
#include <utility>

#include "planner/operator/logical_comment_on.h"
#include "planner/operator/logical_operator.h"
#include "processor/data_pos.h"
#include "processor/operator/comment_on.h"
#include "processor/operator/physical_operator.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCommentOn(
    planner::LogicalOperator* logicalOperator) {
    auto logicalCommentOn = reinterpret_cast<LogicalCommentOn*>(logicalOperator);
    auto outSchema = logicalCommentOn->getSchema();
    auto outputExpression = logicalCommentOn->getOutputExpression();
    auto outputPos = DataPos(outSchema->getExpressionPos(*outputExpression));
    auto commentOnInfo = std::make_unique<CommentOnInfo>(logicalCommentOn->getTableID(),
        logicalCommentOn->getTableName(), logicalCommentOn->getComment(), outputPos, catalog);
    return std::make_unique<CommentOn>(
        std::move(commentOnInfo), getOperatorID(), logicalCommentOn->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
