#pragma once

#include <unordered_set>
#include <vector>

namespace kuzu {
namespace common {

// Vector that inserts distinct element only.
template<typename T>
struct DistinctVector {
    std::vector<T> values;

    void add(const T& val) {
        if (set.contains(val)) {
            return;
        }
        values.push_back(val);
        set.insert(val);
    }

private:
    std::unordered_set<T> set;
};

} // namespace common
} // namespace kuzu
