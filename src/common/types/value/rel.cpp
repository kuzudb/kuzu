#include "common/types/value/rel.h"

#include "common/constants.h"
#include "common/types/value/value.h"
#include "spdlog/fmt/fmt.h"

namespace kuzu {
namespace common {

std::vector<std::pair<std::string, std::unique_ptr<Value>>> RelVal::getProperties(
    const Value* val) {
    throwIfNotRel(val);
    std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
    auto fieldNames = StructType::getFieldNames(val->dataType.get());
    for (auto i = 0u; i < val->childrenSize; ++i) {
        auto currKey = fieldNames[i];
        if (currKey == InternalKeyword::ID || currKey == InternalKeyword::LABEL ||
            currKey == InternalKeyword::SRC || currKey == InternalKeyword::DST) {
            continue;
        }
        auto currVal = val->children[i]->copy();
        properties.emplace_back(currKey, std::move(currVal));
    }
    return properties;
}

uint64_t RelVal::getNumProperties(const Value* val) {
    throwIfNotRel(val);
    auto fieldNames = StructType::getFieldNames(val->dataType.get());
    return fieldNames.size() - OFFSET;
}

std::string RelVal::getPropertyName(const Value* val, uint64_t index) {
    throwIfNotRel(val);
    auto fieldNames = StructType::getFieldNames(val->dataType.get());
    if (index >= fieldNames.size() - OFFSET) {
        return "";
    }
    return fieldNames[index + OFFSET];
}

Value* RelVal::getPropertyVal(const Value* val, uint64_t index) {
    throwIfNotRel(val);
    auto fieldNames = StructType::getFieldNames(val->dataType.get());
    if (index >= fieldNames.size() - OFFSET) {
        return nullptr;
    }
    return val->children[index + OFFSET].get();
}

Value* RelVal::getSrcNodeIDVal(const Value* val) {
    auto fieldIdx = StructType::getFieldIdx(val->dataType.get(), InternalKeyword::SRC);
    return val->children[fieldIdx].get();
}

Value* RelVal::getDstNodeIDVal(const Value* val) {
    auto fieldIdx = StructType::getFieldIdx(val->dataType.get(), InternalKeyword::DST);
    return val->children[fieldIdx].get();
}

nodeID_t RelVal::getSrcNodeID(const Value* val) {
    throwIfNotRel(val);
    auto srcNodeIDVal = getSrcNodeIDVal(val);
    return srcNodeIDVal->getValue<nodeID_t>();
}

nodeID_t RelVal::getDstNodeID(const Value* val) {
    throwIfNotRel(val);
    auto dstNodeIDVal = getDstNodeIDVal(val);
    return dstNodeIDVal->getValue<nodeID_t>();
}

std::string RelVal::getLabelName(const Value* val) {
    auto fieldIdx = StructType::getFieldIdx(val->dataType.get(), InternalKeyword::LABEL);
    return val->children[fieldIdx]->getValue<std::string>();
}

std::string RelVal::toString(const Value* val) {
    throwIfNotRel(val);
    return val->toString();
}

void RelVal::throwIfNotRel(const Value* val) {
    if (val->dataType->getLogicalTypeID() != LogicalTypeID::REL) {
        auto actualType = LogicalTypeUtils::dataTypeToString(val->dataType->getLogicalTypeID());
        throw Exception(fmt::format("Expected REL type, but got {} type", actualType));
    }
}

} // namespace common
} // namespace kuzu
