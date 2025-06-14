#pragma once

#include "common/vector/value_vector.h"
#include <simsimd.h>
#include "torch/torch.h"
namespace kuzu {
namespace function {

struct ArraySquaredDistance {
    template<std::floating_point T>
    static inline void operation(common::list_entry_t& left, common::list_entry_t& right, T& result,
        common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& /*resultVector*/) {
        auto leftElements = (T*)common::ListVector::getListValues(&leftVector, left);
        auto rightElements = (T*)common::ListVector::getListValues(&rightVector, right);
        KU_ASSERT(left.size == right.size);
        static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
        // Get MPS device if available
        auto device = torch::Device(torch::kMPS);

        // Create Torch tensors from raw pointers
        auto options = torch::TensorOptions().dtype(torch::CppTypeToScalarType<T>()).device(device);

        // from_blob still uses host memory, so we need to copy it to MPS
        auto leftTensorHost = torch::from_blob(leftElements, {static_cast<int64_t>(left.size)},
            options.device(torch::kCPU));
        auto rightTensorHost = torch::from_blob(rightElements, {static_cast<int64_t>(right.size)},
            options.device(torch::kCPU));

        auto leftTensor = leftTensorHost.to(device);
        auto rightTensor = rightTensorHost.to(device);

        // Compute squared L2 distance
        auto diff = leftTensor - rightTensor;
        auto squaredDiff = torch::sum(diff * diff);
        auto resultValue = squaredDiff.template item<T>();
        result = resultValue;
    }
};

} // namespace function
} // namespace kuzu
