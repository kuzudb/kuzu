#include "common/types/value/node.h"

#include "common/constants.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "spdlog/fmt/fmt.h"

namespace kuzu {
namespace common {

std::vector<std::pair<std::string, std::unique_ptr<Value>>> NodeVal::getProperties(
    const Value* val) {
    throwIfNotNode(val);
    std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
    auto fieldNames = StructType::getFieldNames(val->dataType.get());
    for (auto i = 0u; i < val->childrenSize; ++i) {
        auto currKey = fieldNames[i];
        if (currKey == InternalKeyword::ID || currKey == InternalKeyword::LABEL) {
            continue;
        }
        properties.emplace_back(currKey, val->children[i]->copy());
    }
    return properties;
}

uint64_t NodeVal::getNumProperties(const Value* val) {
    throwIfNotNode(val);
    auto fieldNames = StructType::getFieldNames(val->dataType.get());
    return fieldNames.size() - OFFSET;
}

std::string NodeVal::getPropertyName(const Value* val, uint64_t index) {
    throwIfNotNode(val);
    auto fieldNames = StructType::getFieldNames(val->dataType.get());
    if (index >= fieldNames.size() - OFFSET) {
        return "";
    }
    return fieldNames[index + OFFSET];
}

Value* NodeVal::getPropertyVal(const Value* val, uint64_t index) {
    throwIfNotNode(val);
    auto fieldNames = StructType::getFieldNames(val->dataType.get());
    if (index >= fieldNames.size() - OFFSET) {
        return nullptr;
    }
    return val->children[index + OFFSET].get();
}

Value* NodeVal::getNodeIDVal(const Value* val) {
    throwIfNotNode(val);
    auto fieldIdx = StructType::getFieldIdx(val->dataType.get(), InternalKeyword::ID);
    return val->children[fieldIdx].get();
}

Value* NodeVal::getLabelVal(const Value* val) {
    throwIfNotNode(val);
    auto fieldIdx = StructType::getFieldIdx(val->dataType.get(), InternalKeyword::LABEL);
    return val->children[fieldIdx].get();
}

nodeID_t NodeVal::getNodeID(const Value* val) {
    throwIfNotNode(val);
    auto nodeIDVal = getNodeIDVal(val);
    return nodeIDVal->getValue<nodeID_t>();
}

std::string NodeVal::getLabelName(const Value* val) {
    throwIfNotNode(val);
    auto labelVal = getLabelVal(val);
    return labelVal->getValue<std::string>();
}

std::string NodeVal::toString(const Value* val) {
    throwIfNotNode(val);
    return val->toString();
}

void NodeVal::throwIfNotNode(const Value* val) {
    if (val->dataType->getLogicalTypeID() != LogicalTypeID::NODE) {
        auto actualType = LogicalTypeUtils::dataTypeToString(val->dataType->getLogicalTypeID());
        throw Exception(fmt::format("Expected NODE type, but got {} type", actualType));
    }
}

} // namespace common
} // namespace kuzu
