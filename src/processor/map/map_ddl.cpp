#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_sequence.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_drop_sequence.h"
#include "planner/operator/ddl/logical_drop_table.h"
#include "processor/operator/ddl/alter.h"
#include "processor/operator/ddl/create_sequence.h"
#include "processor/operator/ddl/create_table.h"
#include "processor/operator/ddl/drop_sequence.h"
#include "processor/operator/ddl/drop_table.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static DataPos getOutputPos(LogicalDDL* logicalDDL) {
    auto outSchema = logicalDDL->getSchema();
    auto outputExpression = logicalDDL->getOutputExpression();
    return DataPos(outSchema->getExpressionPos(*outputExpression));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateTable(LogicalOperator* logicalOperator) {
    auto createTable = (LogicalCreateTable*)logicalOperator;
    return std::make_unique<CreateTable>(createTable->getInfo()->copy(), getOutputPos(createTable),
        getOperatorID(), createTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateSequence(LogicalOperator* logicalOperator) {
    auto createSequence = (LogicalCreateSequence*)logicalOperator;
    return std::make_unique<CreateSequence>(createSequence->getInfo(), getOutputPos(createSequence),
        getOperatorID(), createSequence->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDropTable(LogicalOperator* logicalOperator) {
    auto dropTable = (LogicalDropTable*)logicalOperator;
    return std::make_unique<DropTable>(dropTable->getTableName(), dropTable->getTableID(),
        getOutputPos(dropTable), getOperatorID(), dropTable->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDropSequence(LogicalOperator* logicalOperator) {
    auto dropSequence = (LogicalDropSequence*)logicalOperator;
    return std::make_unique<DropSequence>(dropSequence->getTableName(),
        dropSequence->getSequenceID(), getOutputPos(dropSequence), getOperatorID(),
        dropSequence->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAlter(LogicalOperator* logicalOperator) {
    auto alter = reinterpret_cast<LogicalAlter*>(logicalOperator);
    std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator;
    if (alter->getInfo()->alterType == AlterType::ADD_PROPERTY) {
        auto& addPropInfo =
            ku_dynamic_cast<const BoundExtraAlterInfo&, const BoundExtraAddPropertyInfo&>(
                *alter->getInfo()->extraInfo);
        defaultValueEvaluator =
            ExpressionMapper::getEvaluator(addPropInfo.defaultValue, alter->getSchema());
    }
    return std::make_unique<Alter>(alter->getInfo()->copy(), std::move(defaultValueEvaluator),
        getOutputPos(alter), getOperatorID(), alter->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
