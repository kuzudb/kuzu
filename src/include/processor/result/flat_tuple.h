#pragma once

#include "common/types/value.h"

namespace kuzu {
namespace processor {

class FlatTuple {
public:
    inline void addValue(std::unique_ptr<common::Value> value) {
        values.push_back(std::move(value));
    }

    inline uint32_t len() { return values.size(); }

    common::Value* getValue(uint32_t idx);

    std::string toString(const std::vector<uint32_t>& colsWidth, const std::string& delimiter = "|",
        uint32_t maxWidth = -1);

private:
    std::vector<std::unique_ptr<common::Value>> values;
};

} // namespace processor
} // namespace kuzu
