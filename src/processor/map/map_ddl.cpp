#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_sequence.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_create_type.h"
#include "planner/operator/ddl/logical_drop.h"
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

static DataPos getOutputPos(const LogicalDDL& logicalDDL) {
    auto outSchema = logicalDDL.getSchema();
    auto outputExpression = logicalDDL.getOutputExpression();
    return DataPos(outSchema->getExpressionPos(*outputExpression));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateTable(LogicalOperator* logicalOperator) {
    auto& createTable = logicalOperator->constCast<LogicalCreateTable>();
    auto printInfo = std::make_unique<LogicalCreateTablePrintInfo>(createTable.getInfo()->copy());
    return std::make_unique<CreateTable>(createTable.getInfo()->copy(), getOutputPos(createTable),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateType(LogicalOperator* logicalOperator) {
    auto& createType = logicalOperator->constCast<LogicalCreateType>();
    auto printInfo = std::make_unique<CreateTypePrintInfo>(createType.getTableName(),
        createType.getType().toString());
    return std::make_unique<CreateType>(createType.getTableName(), createType.getType().copy(),
        getOutputPos(createType), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateSequence(LogicalOperator* logicalOperator) {
    auto& createSequence = logicalOperator->constCast<LogicalCreateSequence>();
    auto printInfo = std::make_unique<CreateSequencePrintInfo>(createSequence.getTableName());
    return std::make_unique<CreateSequence>(createSequence.getInfo(), getOutputPos(createSequence),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDrop(LogicalOperator* logicalOperator) {
    auto& drop = logicalOperator->constCast<LogicalDrop>();
    auto& dropInfo = drop.getDropInfo();
    auto printInfo = std::make_unique<DropPrintInfo>(drop.getDropInfo().name);
    return std::make_unique<Drop>(dropInfo, getOutputPos(drop), getOperatorID(),
        std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAlter(LogicalOperator* logicalOperator) {
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
