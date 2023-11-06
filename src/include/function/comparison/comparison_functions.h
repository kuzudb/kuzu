#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct Equals {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result,
        common::ValueVector* /*leftVector*/, common::ValueVector* /*rightVector*/) {
        result = left == right;
    }
};

struct NotEquals {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result,
        common::ValueVector* leftVector, common::ValueVector* rightVector) {
        Equals::operation(left, right, result, leftVector, rightVector);
        result = !result;
    }
};

struct GreaterThan {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result,
        common::ValueVector* /*leftVector*/, common::ValueVector* /*rightVector*/) {
        result = left > right;
    }
};

struct GreaterThanEquals {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result,
        common::ValueVector* leftVector, common::ValueVector* rightVector) {
        uint8_t isGreater;
        uint8_t isEqual;
        GreaterThan::operation(left, right, isGreater, leftVector, rightVector);
        Equals::operation(left, right, isEqual, leftVector, rightVector);
        result = isGreater || isEqual;
    }
};

struct LessThan {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result,
        common::ValueVector* leftVector, common::ValueVector* rightVector) {
        GreaterThanEquals::operation(left, right, result, leftVector, rightVector);
        result = !result;
    }
};

struct LessThanEquals {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result,
        common::ValueVector* leftVector, common::ValueVector* rightVector) {
        GreaterThan::operation(left, right, result, leftVector, rightVector);
        result = !result;
    }
};

// specialization for equal and greater than.
template<>
void Equals::operation(const common::list_entry_t& left, const common::list_entry_t& right,
    uint8_t& result, common::ValueVector* leftVector, common::ValueVector* rightVector);
template<>
void Equals::operation(const common::struct_entry_t& left, const common::struct_entry_t& right,
    uint8_t& result, common::ValueVector* leftVector, common::ValueVector* rightVector);
template<>
void GreaterThan::operation(const common::list_entry_t& left, const common::list_entry_t& right,
    uint8_t& result, common::ValueVector* leftVector, common::ValueVector* rightVector);
template<>
void GreaterThan::operation(const common::struct_entry_t& left, const common::struct_entry_t& right,
    uint8_t& result, common::ValueVector* leftVector, common::ValueVector* rightVector);

} // namespace function
} // namespace kuzu
