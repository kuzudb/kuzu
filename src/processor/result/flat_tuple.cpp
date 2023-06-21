#include "processor/result/flat_tuple.h"

#include "common/string_utils.h"
#include "utf8proc.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace processor {

void FlatTuple::addValue(std::unique_ptr<common::Value> value) {
    values.push_back(std::move(value));
}

uint32_t FlatTuple::len() const {
    return values.size();
}

common::Value* FlatTuple::getValue(uint32_t idx) const {
    if (idx >= len()) {
        throw common::RuntimeException(common::StringUtils::string_format(
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

std::string FlatTuple::toString(
    const std::vector<uint32_t>& colsWidth, const std::string& delimiter, const uint32_t maxWidth) {
    std::ostringstream result;
    for (auto i = 0ul; i < values.size(); i++) {
        std::string value = values[i]->toString();
        if (value.length() > maxWidth) {
            value = value.substr(0, maxWidth - 3) + "...";
        }
        if (colsWidth[i] != 0) {
            value = " " + value + " ";
        }
        uint32_t fieldLen = 0;
        uint32_t chrIter = 0;
        while (chrIter < value.length()) {
            fieldLen += Utf8Proc::renderWidth(value.c_str(), chrIter);
            chrIter = utf8proc_next_grapheme(value.c_str(), value.length(), chrIter);
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
