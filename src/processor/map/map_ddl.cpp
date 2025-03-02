#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_sequence.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_create_type.h"
#include "planner/operator/ddl/logical_drop.h"
#include "processor/expression_mapper.h"
#include "processor/operator/ddl/alter.h"
#include "processor/operator/ddl/create_sequence.h"
#include "processor/operator/ddl/create_table.h"
#include "processor/operator/ddl/create_type.h"
#include "processor/operator/ddl/drop.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static DataPos getOutputPos(const LogicalSimple& simple) {
    auto outSchema = simple.getSchema();
    auto outputExpression = simple.getOutputExpression();
    return DataPos(outSchema->getExpressionPos(*outputExpression));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateTable(
    const LogicalOperator* logicalOperator) {
    auto& createTable = logicalOperator->constCast<LogicalCreateTable>();
    auto printInfo = std::make_unique<LogicalCreateTablePrintInfo>(createTable.getInfo()->copy());
    return std::make_unique<CreateTable>(createTable.getInfo()->copy(), getOutputPos(createTable),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateType(
    const LogicalOperator* logicalOperator) {
    auto& createType = logicalOperator->constCast<LogicalCreateType>();
    auto typeName = createType.getExpressionsForPrinting();
    auto printInfo =
        std::make_unique<CreateTypePrintInfo>(typeName, createType.getType().toString());
    return std::make_unique<CreateType>(typeName, createType.getType().copy(),
        getOutputPos(createType), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateSequence(
    const LogicalOperator* logicalOperator) {
    auto& createSequence = logicalOperator->constCast<LogicalCreateSequence>();
    auto printInfo =
        std::make_unique<CreateSequencePrintInfo>(createSequence.getInfo().sequenceName);
    return std::make_unique<CreateSequence>(createSequence.getInfo(), getOutputPos(createSequence),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDrop(const LogicalOperator* logicalOperator) {
    auto& drop = logicalOperator->constCast<LogicalDrop>();
    auto& dropInfo = drop.getDropInfo();
    auto printInfo = std::make_unique<DropPrintInfo>(drop.getDropInfo().name);
    return std::make_unique<Drop>(dropInfo, getOutputPos(drop), getOperatorID(),
        std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAlter(const LogicalOperator* logicalOperator) {
    auto& alter = logicalOperator->constCast<LogicalAlter>();
    std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator;
    auto exprMapper = ExpressionMapper(alter.getSchema());
    if (alter.getInfo()->alterType == AlterType::ADD_PROPERTY) {
        auto& addPropInfo = alter.getInfo()->extraInfo->constCast<BoundExtraAddPropertyInfo>();
        defaultValueEvaluator = exprMapper.getEvaluator(addPropInfo.boundDefault);
    }
    auto printInfo = std::make_unique<LogicalAlterPrintInfo>(alter.getInfo()->copy());
    return std::make_unique<Alter>(alter.getInfo()->copy(), std::move(defaultValueEvaluator),
        getOutputPos(alter), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
