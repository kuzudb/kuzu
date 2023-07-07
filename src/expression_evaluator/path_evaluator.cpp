#include "expression_evaluator/path_evaluator.h"

#include "binder/expression/path_expression.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace evaluator {

// For each result field vector, find its corresponding input field vector if exist.
static std::vector<common::ValueVector*> getFieldVectors(
    const LogicalType& inputType, const LogicalType& resultType, common::ValueVector* inputVector) {
    std::vector<common::ValueVector*> result;
    for (auto field : StructType::getFields(&resultType)) {
        auto fieldName = StringUtils::getUpper(field->getName());
        if (StructType::hasField(&inputType, fieldName)) {
            auto idx = StructType::getFieldIdx(&inputType, fieldName);
            result.push_back(common::StructVector::getFieldVector(inputVector, idx).get());
        } else {
            result.push_back(nullptr);
        }
    }
    return result;
}

void PathExpressionEvaluator::init(
    const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) {
    BaseExpressionEvaluator::init(resultSet, memoryManager);
    auto resultNodesIdx = StructType::getFieldIdx(&resultVector->dataType, InternalKeyword::NODES);
    resultNodesVector = StructVector::getFieldVector(resultVector.get(), resultNodesIdx).get();
    auto resultNodesDataVector = ListVector::getDataVector(resultNodesVector);
    for (auto& fieldVector : StructVector::getFieldVectors(resultNodesDataVector)) {
        resultNodesFieldVectors.push_back(fieldVector.get());
    }
    auto resultRelsIdx = StructType::getFieldIdx(&resultVector->dataType, InternalKeyword::RELS);
    resultRelsVector = StructVector::getFieldVector(resultVector.get(), resultRelsIdx).get();
    auto resultRelsDataVector = ListVector::getDataVector(resultRelsVector);
    for (auto& fieldVector : StructVector::getFieldVectors(resultRelsDataVector)) {
        resultRelsFieldVectors.push_back(fieldVector.get());
    }
    for (auto i = 0u; i < pathExpression->getNumChildren(); ++i) {
        auto child = pathExpression->getChild(i).get();
        auto vectors = std::make_unique<InputVectors>();
        vectors->input = children[i]->resultVector.get();
        switch (child->dataType.getLogicalTypeID()) {
        case common::LogicalTypeID::NODE: {
            vectors->nodeFieldVectors = getFieldVectors(
                child->dataType, pathExpression->getNode()->dataType, vectors->input);
        } break;
        case common::LogicalTypeID::REL: {
            vectors->relFieldVectors = getFieldVectors(
                child->dataType, pathExpression->getRel()->dataType, vectors->input);
        } break;
        case common::LogicalTypeID::RECURSIVE_REL: {
            auto rel = (RelExpression*)child;
            auto recursiveNode = rel->getRecursiveInfo()->node;
            auto recursiveRel = rel->getRecursiveInfo()->rel;
            auto nodeFieldIdx = StructType::getFieldIdx(&child->dataType, InternalKeyword::NODES);
            vectors->nodesInput = StructVector::getFieldVector(vectors->input, nodeFieldIdx).get();
            vectors->nodesDataInput = ListVector::getDataVector(vectors->nodesInput);
            vectors->nodeFieldVectors = getFieldVectors(recursiveNode->dataType,
                pathExpression->getNode()->getDataType(), vectors->nodesDataInput);
            auto relFieldIdx =
                StructType::getFieldIdx(&vectors->input->dataType, InternalKeyword::RELS);
            vectors->relsInput = StructVector::getFieldVector(vectors->input, relFieldIdx).get();
            vectors->relsDataInput = ListVector::getDataVector(vectors->relsInput);
            vectors->relFieldVectors = getFieldVectors(recursiveRel->dataType,
                pathExpression->getRel()->getDataType(), vectors->relsDataInput);
        } break;
        default:
            throw common::NotImplementedException("PathExpressionEvaluator::init");
        }
        inputVectorsPerChild.push_back(std::move(vectors));
    }
}

void PathExpressionEvaluator::evaluate() {
    for (auto& child : children) {
        child->evaluate();
    }
    auto selVector = resultVector->state->selVector;
    for (auto i = 0; i < selVector->selectedSize; ++i) {
        auto pos = selVector->selectedPositions[i];
        copyNodes(pos);
        copyRels(pos);
    }
}

static inline uint32_t getCurrentPos(common::ValueVector* vector, uint32_t pos) {
    if (vector->state->isFlat()) {
        return vector->state->selVector->selectedPositions[0];
    }
    return pos;
}

void PathExpressionEvaluator::copyNodes(common::sel_t resultPos) {
    auto listSize = 0u;
    // Calculate list size.
    for (auto i = 0; i < pathExpression->getNumChildren(); ++i) {
        auto child = pathExpression->getChild(i).get();
        switch (child->dataType.getLogicalTypeID()) {
        case common::LogicalTypeID::NODE: {
            listSize++;
        } break;
        case common::LogicalTypeID::RECURSIVE_REL: {
            auto vectors = inputVectorsPerChild[i].get();
            auto inputPos = getCurrentPos(vectors->input, resultPos);
            listSize += vectors->nodesInput->getValue<common::list_entry_t>(inputPos).size;
        } break;
        default:
            break;
        }
    }
    // Add list entry.
    auto entry = common::ListVector::addList(resultNodesVector, listSize);
    resultNodesVector->setValue(resultPos, entry);
    // Copy field vectors
    common::offset_t resultDataPos = entry.offset;
    for (auto i = 0; i < pathExpression->getNumChildren(); ++i) {
        auto child = pathExpression->getChild(i).get();
        auto vectors = inputVectorsPerChild[i].get();
        auto inputPos = getCurrentPos(vectors->input, resultPos);
        switch (child->dataType.getLogicalTypeID()) {
        case common::LogicalTypeID::NODE: {
            copyFieldVectors(
                inputPos, vectors->nodeFieldVectors, resultDataPos, resultNodesFieldVectors);
        } break;
        case common::LogicalTypeID::RECURSIVE_REL: {
            auto& listEntry = vectors->nodesInput->getValue<common::list_entry_t>(inputPos);
            for (auto j = 0; j < listEntry.size; ++j) {
                copyFieldVectors(listEntry.offset + j, vectors->nodeFieldVectors, resultDataPos,
                    resultNodesFieldVectors);
            }
        } break;
        default:
            break;
        }
    }
}

void PathExpressionEvaluator::copyRels(common::sel_t resultPos) {
    auto listSize = 0u;
    // Calculate list size.
    for (auto i = 0; i < pathExpression->getNumChildren(); ++i) {
        auto child = pathExpression->getChild(i).get();
        switch (child->dataType.getLogicalTypeID()) {
        case common::LogicalTypeID::REL: {
            listSize++;
        } break;
        case common::LogicalTypeID::RECURSIVE_REL: {
            auto vectors = inputVectorsPerChild[i].get();
            auto inputPos = getCurrentPos(vectors->input, resultPos);
            listSize += vectors->relsInput->getValue<common::list_entry_t>(inputPos).size;
        } break;
        default:
            break;
        }
    }
    // Add list entry.
    auto entry = common::ListVector::addList(resultRelsVector, listSize);
    resultRelsVector->setValue(resultPos, entry);
    // Copy field vectors
    common::offset_t resultDataPos = entry.offset;
    for (auto i = 0; i < pathExpression->getNumChildren(); ++i) {
        auto child = pathExpression->getChild(i).get();
        auto vectors = inputVectorsPerChild[i].get();
        auto inputPos = getCurrentPos(vectors->input, resultPos);
        switch (child->dataType.getLogicalTypeID()) {
        case common::LogicalTypeID::REL: {
            copyFieldVectors(
                inputPos, vectors->relFieldVectors, resultDataPos, resultRelsFieldVectors);
        } break;
        case common::LogicalTypeID::RECURSIVE_REL: {
            auto& listEntry = vectors->relsInput->getValue<common::list_entry_t>(inputPos);
            for (auto j = 0; j < listEntry.size; ++j) {
                copyFieldVectors(listEntry.offset + j, vectors->relFieldVectors, resultDataPos,
                    resultRelsFieldVectors);
            }
        } break;
        default:
            break;
        }
    }
}

void PathExpressionEvaluator::copyFieldVectors(common::offset_t inputVectorPos,
    const std::vector<common::ValueVector*>& inputFieldVectors, common::offset_t& resultVectorPos,
    const std::vector<common::ValueVector*>& resultFieldVectors) {
    assert(resultFieldVectors.size() == inputFieldVectors.size());
    for (auto i = 0u; i < inputFieldVectors.size(); ++i) {
        auto inputFieldVector = inputFieldVectors[i];
        auto resultFieldVector = resultFieldVectors[i];
        if (inputFieldVector == nullptr || inputFieldVector->isNull(inputVectorPos)) {
            resultFieldVector->setNull(resultVectorPos, true);
            continue;
        }
        resultFieldVector->setNull(resultVectorPos, false);
        assert(inputFieldVector->dataType == resultFieldVector->dataType);
        resultFieldVector->copyFromVectorData(resultVectorPos, inputFieldVector, inputVectorPos);
    }
    resultVectorPos++;
}

void PathExpressionEvaluator::resolveResultVector(
    const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(pathExpression->getDataType(), memoryManager);
    std::vector<BaseExpressionEvaluator*> inputEvaluators;
    inputEvaluators.reserve(children.size());
    for (auto& child : children) {
        inputEvaluators.push_back(child.get());
    }
    resolveResultStateFromChildren(inputEvaluators);
}

} // namespace evaluator
} // namespace kuzu
