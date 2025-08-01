#include "processor/result/flat_tuple.h"

#include <sstream>

#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/types/value/value.h"
#include "utf8proc.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void FlatTuple::addValue(std::unique_ptr<Value> value) {
    values.push_back(std::move(value));
}

uint32_t FlatTuple::len() const {
    return values.size();
}

Value* FlatTuple::getValue(uint32_t idx) const {
    if (idx >= len()) {
        throw RuntimeException(stringFormat(
            "ValIdx is out of range. Number of values in flatTuple: {}, valIdx: {}.", len(), idx));
    }
    return values[idx].get();
}

std::string FlatTuple::toString() {
    std::string result;
    for (auto i = 0ul; i < values.size(); i++) {
        if (i != 0) {
            result += "|";
        }
        result += values[i]->toString();
    }
    result += "\n";
    return result;
}

std::string FlatTuple::toString(const std::vector<uint32_t>& colsWidth,
    const std::string& delimiter, const uint32_t maxWidth) {
    std::ostringstream result;
    for (auto i = 0ul; i < values.size(); i++) {
        auto value = values[i]->toString();
        auto fieldLen = 0u;
        auto cutoff = 0u, cutoffLen = 0u;
        for (auto iter = 0u; iter < value.length();) {
            auto width = Utf8Proc::renderWidth(value.c_str(), iter);
            if (fieldLen + 3 > maxWidth && cutoff == 0) {
                cutoff = iter;
                cutoffLen = fieldLen;
            }
            fieldLen += width;
            iter = utf8proc_next_grapheme(value.c_str(), value.length(), iter);
        }
        if (fieldLen > maxWidth) {
            value = value.substr(0, cutoff) + "...";
            fieldLen = cutoffLen + 3;
        }
        if (colsWidth[i] != 0) {
            value = " " + std::move(value) + " ";
            fieldLen += 2;
        }
        fieldLen = std::min(fieldLen, maxWidth + 2);
        if (colsWidth[i] != 0) {
            result << value << std::string(colsWidth[i] - fieldLen, ' ');
        } else {
            result << value;
        }
        if (i != values.size() - 1) {
            result << delimiter;
        }
    }
    return result.str();
}

} // namespace processor
} // namespace kuzu
