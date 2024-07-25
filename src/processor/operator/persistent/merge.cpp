#include "processor/operator/persistent/merge.h"

#include "binder/expression/expression_util.h"

namespace kuzu {
namespace processor {

std::string MergePrintInfo::toString() const {
    std::string result = "Pattern: ";
    result += binder::ExpressionUtil::toString(pattern);
    if (!onMatch.empty()) {
        result += ", ON MATCH SET: " + binder::ExpressionUtil::toString(onMatch);
    }
    if (!onCreate.empty()) {
        result += ", ON CREATE SET: " + binder::ExpressionUtil::toString(onCreate);
    }
    return result;
}

void Merge::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* context) {
    for (auto& executor : nodeInsertExecutors) {
        executor.init(resultSet, context);
    }
    for (auto& executor : relInsertExecutors) {
        executor.init(resultSet, context);
    }
    for (auto& executor : onCreateNodeSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onCreateRelSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onMatchNodeSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onMatchRelSetExecutors) {
        executor->init(resultSet, context);
    }
    localState.init(*resultSet, context->clientContext, info);
}

void MergeLocalState::init(ResultSet& resultSet, main::ClientContext* context, MergeInfo& info) {
    std::vector<common::LogicalType> types;
    for (auto& keyPos : info.keyPoses) {
        auto keyVector = resultSet.getValueVector(keyPos).get();
        types.push_back(keyVector->dataType.copy());
        keyVectors.push_back(keyVector);
    }
    hashTable = std::make_unique<PatternCreationInfoTable>(*context->getMemoryManager(),
        std::move(types), std::move(info.tableSchema));
    existenceVector = resultSet.getValueVector(info.existenceMark).get();
}

bool MergeLocalState::patternExists() const {
    KU_ASSERT(existenceVector->state->getSelVector().getSelSize() == 1);
    auto pos = existenceVector->state->getSelVector()[0];
    return existenceVector->getValue<bool>(pos);
}

void Merge::executeOnMatch(ExecutionContext* context) {
    for (auto& executor : onMatchNodeSetExecutors) {
        executor->set(context);
    }
    for (auto& executor : onMatchRelSetExecutors) {
        executor->set(context);
    }
}

void Merge::executeOnCreatedPattern(PatternCreationInfo& patternCreationInfo,
    ExecutionContext* context) {
    for (auto& executor : nodeInsertExecutors) {
        executor.skipInsert();
    }
    for (auto& executor : relInsertExecutors) {
        executor.skipInsert();
    }
    for (auto i = 0u; i < onMatchNodeSetExecutors.size(); i++) {
        auto& executor = onMatchNodeSetExecutors[i];
        auto nodeIDToSet = patternCreationInfo.getPatternID(i);
        executor->setNodeID(nodeIDToSet);
        executor->set(context);
    }
    for (auto i = 0u; i < onMatchRelSetExecutors.size(); i++) {
        auto& executor = onMatchRelSetExecutors[i];
        auto relIDToSet = patternCreationInfo.getPatternID(i + onMatchNodeSetExecutors.size());
        executor->setRelID(relIDToSet);
        executor->set(context);
    }
}

void Merge::executeOnNewPattern(PatternCreationInfo& patternCreationInfo,
    ExecutionContext* context) {
    // do insert and on create
    for (auto i = 0u; i < nodeInsertExecutors.size(); i++) {
        auto& executor = nodeInsertExecutors[i];
        executor.setNodeIDVectorToNonNull();
        auto nodeID = executor.insert(context->clientContext->getTx());
        patternCreationInfo.updateID(i, info.executorInfo, nodeID);
    }
    for (auto i = 0u; i < relInsertExecutors.size(); i++) {
        auto& executor = relInsertExecutors[i];
        auto relID = executor.insert(context->clientContext->getTx());
        patternCreationInfo.updateID(i + nodeInsertExecutors.size(), info.executorInfo, relID);
    }
    for (auto& executor : onCreateNodeSetExecutors) {
        executor->set(context);
    }
    for (auto& executor : onCreateRelSetExecutors) {
        executor->set(context);
    }
}

void Merge::executeNoMatch(ExecutionContext* context) {
    auto patternCreationInfo = localState.getPatternCreationInfo();
    if (patternCreationInfo.hasCreated) {
        executeOnCreatedPattern(patternCreationInfo, context);
    } else {
        executeOnNewPattern(patternCreationInfo, context);
    }
}

bool Merge::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    if (localState.patternExists()) {
        executeOnMatch(context);
    } else {
        executeNoMatch(context);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> Merge::clone() {
    return std::make_unique<Merge>(copyVector(nodeInsertExecutors), copyVector(relInsertExecutors),
        copyVector(onCreateNodeSetExecutors), copyVector(onCreateRelSetExecutors),
        copyVector(onMatchNodeSetExecutors), copyVector(onMatchRelSetExecutors), info.copy(),
        children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu
