#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct MapCreationFunctions {
    static constexpr const char* name = "MAP";

    static function_set getFunctionSet();
};

struct MapExtractFunctions {
    static constexpr const char* name = "MAP_EXTRACT";

    static constexpr const char* alias = "ELEMENT_AT";

    static function_set getFunctionSet();
};

struct MapKeysFunctions {
    static constexpr const char* name = "MAP_KEYS";

    static function_set getFunctionSet();
};

struct MapValuesFunctions {
    static constexpr const char* name = "MAP_VALUES";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
