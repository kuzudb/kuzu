#pragma once

#include <vector>

#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Levenshtein {
public:
    static void operation(common::ku_string_t& left, common::ku_string_t& right, int64_t& result) {
        // If one string is empty, the distance equals the length of the other string.
        if (left.len == 0 || right.len == 0) {
            result = left.len + right.len;
            return;
        }

        auto leftStr = left.getData();
        auto rightStr = right.getData();

        std::vector<uint64_t> distances0(right.len + 1, 0);
        std::vector<uint64_t> distances1(right.len + 1, 0);

        uint64_t substitutionCost = 0;
        uint64_t insertionCost = 0;
        uint64_t deletionCost = 0;

        for (auto i = 0u; i <= right.len; i++) {
            distances0[i] = i;
        }

        for (auto i = 0u; i < left.len; i++) {
            distances1[0] = i + 1;
            for (auto j = 0u; j < right.len; j++) {
                deletionCost = distances0[j + 1] + 1;
                insertionCost = distances1[j] + 1;
                substitutionCost = distances0[j];

                if (leftStr[i] != rightStr[j]) {
                    substitutionCost += 1;
                }
                distances1[j + 1] =
                    std::min(deletionCost, std::min(substitutionCost, insertionCost));
            }
            // Copy distances1 (current row) to distances0 (previous row) for next iteration
            // since data in distances1 is always invalidated, a swap without copy is more
            // efficient.
            distances0 = distances1;
        }
        result = distances0[right.len];
    }
};

} // namespace function
} // namespace kuzu
