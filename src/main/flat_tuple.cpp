#include "main/flat_tuple.h"

#include "utf8proc.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace processor {

common::Value* FlatTuple::getValue(uint32_t idx) {
    if (idx >= len()) {
        throw common::RuntimeException(common::StringUtils::string_format(
            "ValIdx is out of range. Number of values in flatTuple: %d, valIdx: %d.", len(), idx));
    }
    return values[idx].get();
}

string FlatTuple::toString(
    const vector<uint32_t>& colsWidth, const string& delimiter, const uint32_t maxWidth) {
    ostringstream result;
    for (auto i = 0ul; i < values.size(); i++) {
        string value = values[i]->toString();
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
        fieldLen = min(fieldLen, maxWidth + 2);
        if (colsWidth[i] != 0) {
            result << value << string(colsWidth[i] - fieldLen, ' ');
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
