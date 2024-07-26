#pragma once

#include "int128_t.h"

namespace kuzu {
namespace common {

struct KUZU_API decimal_t {

    int128_t val = 0;
    uint32_t precision = 18;
    uint32_t scale = 3;

    decimal_t() {}
    decimal_t(int128_t val, uint32_t prec, uint32_t scale):
        val(val), precision(prec), scale(scale) {}
};

}
}
