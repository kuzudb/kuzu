#pragma once

#include "common/api.h"
#include "common/types/value.h"

namespace kuzu {
namespace processor {

class FlatTuple {
public:
    void addValue(std::unique_ptr<common::Value> value);

    KUZU_API uint32_t len();

    KUZU_API common::Value* getValue(uint32_t idx);

    KUZU_API std::string toString(const std::vector<uint32_t>& colsWidth,
        const std::string& delimiter = "|", uint32_t maxWidth = -1);

private:
    std::vector<std::unique_ptr<common::Value>> values;
};

} // namespace processor
} // namespace kuzu
