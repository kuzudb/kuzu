#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_sequence.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_create_type.h"
#include "planner/operator/ddl/logical_drop_sequence.h"
#include "planner/operator/ddl/logical_drop_table.h"
#include "processor/operator/ddl/alter.h"
#include "processor/operator/ddl/create_sequence.h"
#include "processor/operator/ddl/create_table.h"
#include "processor/operator/ddl/create_type.h"
#include "processor/operator/ddl/drop_sequence.h"
#include "processor/operator/ddl/drop_table.h"
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
    auto printInfo = std::make_unique<OPPrintInfo>(createTable.getExpressionsForPrinting());
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapDropTable(LogicalOperator* logicalOperator) {
    auto& dropTable = logicalOperator->constCast<LogicalDropTable>();
    auto printInfo = std::make_unique<DropTablePrintInfo>(dropTable.getTableName());
    return std::make_unique<DropTable>(dropTable.getTableName(), dropTable.getTableID(),
        getOutputPos(dropTable), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDropSequence(LogicalOperator* logicalOperator) {
    auto& dropSequence = logicalOperator->constCast<LogicalDropSequence>();
    auto printInfo = std::make_unique<DropSequencePrintInfo>(dropSequence.getTableName());
    return std::make_unique<DropSequence>(dropSequence.getTableName(), dropSequence.getSequenceID(),
        getOutputPos(dropSequence), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAlter(LogicalOperator* logicalOperator) {
    auto& alter = logicalOperator->constCast<LogicalAlter>();
    std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator;
    auto exprMapper = ExpressionMapper(alter.getSchema());
    if (alter.getInfo()->alterType == AlterType::ADD_PROPERTY) {
        auto& addPropInfo = alter.getInfo()->extraInfo->constCast<BoundExtraAddPropertyInfo>();
        defaultValueEvaluator = exprMapper.getEvaluator(addPropInfo.boundDefault);
    }
    auto printInfo = std::make_unique<AlterPrintInfo>(alter.getInfo()->alterType,
        alter.getInfo()->tableName, alter.getInfo()->extraInfo.get());
    return std::make_unique<Alter>(alter.getInfo()->copy(), std::move(defaultValueEvaluator),
        getOutputPos(alter), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
