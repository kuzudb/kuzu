#include "function/comparison/comparison_operations.h"

namespace kuzu {
namespace function {
namespace operation {

template<typename OP>
static void executeNestedOperation(uint8_t& result, common::ValueVector* leftVector,
    common::ValueVector* rightVector, uint64_t leftPos, uint64_t rightPos) {
    switch (leftVector->dataType.getPhysicalType()) {
    case common::PhysicalTypeID::BOOL: {
        OP::operation(leftVector->getValue<uint8_t>(leftPos),
            rightVector->getValue<uint8_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::INT64: {
        OP::operation(leftVector->getValue<int64_t>(leftPos),
            rightVector->getValue<int64_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::INT32: {
        OP::operation(leftVector->getValue<int32_t>(leftPos),
            rightVector->getValue<int32_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::INT16: {
        OP::operation(leftVector->getValue<int16_t>(leftPos),
            rightVector->getValue<int16_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::DOUBLE: {
        OP::operation(leftVector->getValue<double_t>(leftPos),
            rightVector->getValue<double_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::FLOAT: {
        OP::operation(leftVector->getValue<float_t>(leftPos),
            rightVector->getValue<float_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::STRING: {
        OP::operation(leftVector->getValue<common::ku_string_t>(leftPos),
            rightVector->getValue<common::ku_string_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::INTERVAL: {
        OP::operation(leftVector->getValue<common::interval_t>(leftPos),
            rightVector->getValue<common::interval_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::INTERNAL_ID: {
        OP::operation(leftVector->getValue<common::internalID_t>(leftPos),
            rightVector->getValue<common::internalID_t>(rightPos), result, nullptr /* left */,
            nullptr /* right */);
    } break;
    case common::PhysicalTypeID::VAR_LIST: {
        OP::operation(leftVector->getValue<common::list_entry_t>(leftPos),
            rightVector->getValue<common::list_entry_t>(rightPos), result, leftVector, rightVector);
    } break;
    case common::PhysicalTypeID::STRUCT: {
        OP::operation(leftVector->getValue<common::struct_entry_t>(leftPos),
            rightVector->getValue<common::struct_entry_t>(rightPos), result, leftVector,
            rightVector);
    } break;
    default: {
        throw common::NotImplementedException("comparison operation");
    }
    }
}

static void executeNestedEqual(uint8_t& result, common::ValueVector* leftVector,
    common::ValueVector* rightVector, uint64_t leftPos, uint64_t rightPos) {
    if (leftVector->isNull(leftPos) && rightVector->isNull(rightPos)) {
        result = true;
    } else if (leftVector->isNull(leftPos) != rightVector->isNull(rightPos)) {
        result = false;
    } else {
        executeNestedOperation<Equals>(result, leftVector, rightVector, leftPos, rightPos);
    }
}

template<>
void Equals::operation(const common::list_entry_t& left, const common::list_entry_t& right,
    uint8_t& result, common::ValueVector* leftVector, common::ValueVector* rightVector) {
    if (leftVector->dataType != rightVector->dataType || left.size != right.size) {
        result = false;
        return;
    }
    auto leftDataVector = common::ListVector::getDataVector(leftVector);
    auto rightDataVector = common::ListVector::getDataVector(rightVector);
    for (auto i = 0u; i < left.size; i++) {
        auto leftPos = left.offset + i;
        auto rightPos = right.offset + i;
        executeNestedEqual(result, leftDataVector, rightDataVector, leftPos, rightPos);
        if (!result) {
            return;
        }
    }
    result = true;
}

template<>
void Equals::operation(const common::struct_entry_t& left, const common::struct_entry_t& right,
    uint8_t& result, common::ValueVector* leftVector, common::ValueVector* rightVector) {
    if (leftVector->dataType != rightVector->dataType) {
        result = false;
        return;
    }
    auto leftFields = common::StructVector::getFieldVectors(leftVector);
    auto rightFields = common::StructVector::getFieldVectors(rightVector);
    for (auto i = 0u; i < leftFields.size(); i++) {
        auto leftField = leftFields[i].get();
        auto rightField = rightFields[i].get();
        executeNestedEqual(result, leftField, rightField, left.pos, right.pos);
        if (!result) {
            return;
        }
    }
    result = true;
}

static void executeNestedGreaterThan(uint8_t& isGreaterThan, uint8_t& isEqual,
    common::ValueVector* leftDataVector, common::ValueVector* rightDataVector, uint64_t leftPos,
    uint64_t rightPos) {
    auto isLeftNull = leftDataVector->isNull(leftPos);
    auto isRightNull = rightDataVector->isNull(rightPos);
    if (isLeftNull || isRightNull) {
        isGreaterThan = !isRightNull;
        isEqual = (isLeftNull == isRightNull);
    } else {
        executeNestedOperation<GreaterThan>(
            isGreaterThan, leftDataVector, rightDataVector, leftPos, rightPos);
        if (!isGreaterThan) {
            executeNestedOperation<Equals>(
                isEqual, leftDataVector, rightDataVector, leftPos, rightPos);
        } else {
            isEqual = false;
        }
    }
}

template<>
void GreaterThan::operation(const common::list_entry_t& left, const common::list_entry_t& right,
    uint8_t& result, common::ValueVector* leftVector, common::ValueVector* rightVector) {
    assert(leftVector->dataType == rightVector->dataType);
    auto leftDataVector = common::ListVector::getDataVector(leftVector);
    auto rightDataVector = common::ListVector::getDataVector(rightVector);
    auto commonLength = std::min(left.size, right.size);
    uint8_t isEqual;
    for (auto i = 0u; i < commonLength; i++) {
        auto leftPos = left.offset + i;
        auto rightPos = right.offset + i;
        executeNestedGreaterThan(
            result, isEqual, leftDataVector, rightDataVector, leftPos, rightPos);
        if (result || (!result && !isEqual)) {
            return;
        }
    }
    result = left.size > right.size;
}

template<>
void GreaterThan::operation(const common::struct_entry_t& left, const common::struct_entry_t& right,
    uint8_t& result, common::ValueVector* leftVector, common::ValueVector* rightVector) {
    assert(leftVector->dataType == rightVector->dataType);
    auto leftFields = common::StructVector::getFieldVectors(leftVector);
    auto rightFields = common::StructVector::getFieldVectors(rightVector);
    uint8_t isEqual;
    for (auto i = 0u; i < leftFields.size(); i++) {
        auto leftField = leftFields[i].get();
        auto rightField = rightFields[i].get();
        executeNestedGreaterThan(result, isEqual, leftField, rightField, left.pos, right.pos);
        if (result || (!result && !isEqual)) {
            return;
        }
    }
    result = false;
}

} // namespace operation
} // namespace function
} // namespace kuzu
