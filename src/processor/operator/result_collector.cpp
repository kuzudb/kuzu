#include "processor/operator/result_collector.h"

#include "binder/expression/expression_util.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

ResultCollectorSharedState::ResultCollectorSharedState(std::shared_ptr<FactorizedTable> table)
    : table{std::move(table)} {}

void ResultCollectorSharedState::setWarningTable(std::shared_ptr<FactorizedTable> warningTable) {
    this->warningTable = std::move(warningTable);
}

std::shared_ptr<FactorizedTable> ResultCollectorSharedState::getWarningTable() {
    return warningTable;
}

std::string ResultCollectorPrintInfo::toString() const {
    std::string result = "";
    if (accumulateType == AccumulateType::OPTIONAL_) {
        result += "Type: " + AccumulateTypeUtil::toString(accumulateType);
    }
    result += ",Expressions: ";
    result += binder::ExpressionUtil::toString(expressions);
    return result;
}

CollectedQueryResult ResultCollector::getResult() {
    return {sharedState->getTable(), sharedState->getWarningTable()};
}

void ResultCollector::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    payloadVectors.reserve(info.payloadPositions.size());
    for (auto& pos : info.payloadPositions) {
        auto vec = resultSet->getValueVector(pos).get();
        payloadVectors.push_back(vec);
        payloadAndMarkVectors.push_back(vec);
    }
    if (info.accumulateType == AccumulateType::OPTIONAL_) {
        markVector = std::make_unique<ValueVector>(LogicalType::BOOL(),
            context->clientContext->getMemoryManager());
        markVector->state = DataChunkState::getSingleValueDataChunkState();
        markVector->setValue<bool>(0, true);
        payloadAndMarkVectors.push_back(markVector.get());
    }
    localTable = std::make_unique<FactorizedTable>(context->clientContext->getMemoryManager(),
        info.tableSchema.copy());
}

void ResultCollector::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        if (!payloadVectors.empty()) {
            for (auto i = 0u; i < resultSet->multiplicity; i++) {
                localTable->append(payloadAndMarkVectors);
            }
        }
    }
    if (!payloadVectors.empty()) {
        sharedState->mergeLocalTable(*localTable);
    }
}

void ResultCollector::finalizeInternal(ExecutionContext* context) {
    sharedState->setWarningTable(std::move(context->warningContext.warningTable));
    switch (info.accumulateType) {
    case AccumulateType::OPTIONAL_: {
        // We should remove currIdx completely as some of the code still relies on currIdx = -1 to
        // check if the state if unFlat or not. This should no longer be necessary.
        // TODO(Ziyi): add an interface in factorized table
        auto table = sharedState->getTable();
        auto tableSchema = table->getTableSchema();
        for (auto i = 0u; i < payloadVectors.size(); ++i) {
            auto columnSchema = tableSchema->getColumn(i);
            if (columnSchema->isFlat()) {
                payloadVectors[i]->state->setToFlat();
            }
        }
        if (table->isEmpty()) {
            for (auto& vector : payloadVectors) {
                vector->setAsSingleNullEntry();
            }
            markVector->setValue<bool>(0, false);
            table->append(payloadAndMarkVectors);
        }
    }
    default:
        break;
    }
}

} // namespace processor
} // namespace kuzu
