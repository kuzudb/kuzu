#include "include/pattern_element.h"

namespace kuzu {
namespace parser {

bool PatternElement::operator==(const PatternElement& other) const {
    if (*nodePattern != *other.nodePattern ||
        patternElementChains.size() != other.patternElementChains.size()) {
        return false;
    }
    for (auto i = 0u; i < patternElementChains.size(); ++i) {
        if (*patternElementChains[i] != *other.patternElementChains[i]) {
            return false;
        }
    }
    return true;
}

} // namespace parser
} // namespace kuzu
