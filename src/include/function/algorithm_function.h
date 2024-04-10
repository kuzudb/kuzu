#pragma once

#include "function.h"

namespace kuzu {
namespace function {

struct GraphInfo {

};

struct AlgoBindData {
    //
};

using algo_func_bind_t = std::function<std::unique_ptr<AlgoBindData>()>;

struct AlgoFunction : public Function {
    algo_func_bind_t bindFunc;


    static std::unique_ptr<>
};

}
}
