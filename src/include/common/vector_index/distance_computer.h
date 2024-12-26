#pragma once

#include <memory>

#include <common/types/types.h>
#include <simsimd/simsimd.h>

namespace kuzu {
namespace common {

struct DistanceComputer {
    virtual void computeDistance(vector_id_t id, double* result) = 0;

    virtual void computeDistance(vector_id_t src, vector_id_t dest, double* result) = 0;

    virtual void computeDistance(const float* dest, double* result) = 0;

    virtual void batchComputeDistances(const float* dest, int size, double* results) = 0;

    virtual void batchComputeDistances(vector_id_t* ids, double* results, int size) = 0;

    virtual void setQuery(const float* query) = 0;

    virtual std::unique_ptr<DistanceComputer> copy() = 0;
};

struct L2DistanceComputer : public DistanceComputer {
    explicit L2DistanceComputer(const float* data, int dim, int n)
        : data(data), dim(dim), n(n), query(nullptr) {}

    inline void computeDistance(vector_id_t id, double* result) override {
        KU_ASSERT_MSG(id < n, stringFormat("Index out of bounds {} < {}", id, n).data());
        const float* y = data + (id * dim);
        simsimd_l2sq_f32(query, y, dim, result);
    }

    inline void computeDistance(vector_id_t src, vector_id_t dest, double* result) override {
        KU_ASSERT_MSG(src < n && dest < n, "Index out of bounds");
        const float* x = data + (src * dim);
        const float* y = data + (dest * dim);
        simsimd_l2sq_f32(x, y, dim, result);
    }

    inline void computeDistance(const float* dest, double* result) override {
        simsimd_l2sq_f32(query, dest, dim, result);
    }

    inline void batchComputeDistances(const float* dest, int size, double* results) override {
        for (int i = 0; i < size; i++) {
            computeDistance(dest + (i * dim), &results[i]);
        }
    }

    inline void batchComputeDistances(vector_id_t* ids, double* results, int size) override {
        for (int i = 0; i < size; i++) {
            computeDistance(ids[i], &results[i]);
        }
    }

    inline void setQuery(const float* q) override { this->query = q; }

    inline std::unique_ptr<DistanceComputer> copy() override {
        return std::make_unique<L2DistanceComputer>(data, dim, n);
    }

private:
    const float* data;
    int dim;
    int n;

    const float* query;
};

struct CosineDistanceComputer : public DistanceComputer {
    explicit CosineDistanceComputer(const float* data, int dim, int n)
            : data(data), dim(dim), n(n), query(nullptr) {}

    inline void computeDistance(vector_id_t id, double* result) override {
        KU_ASSERT_MSG(id < n, stringFormat("Index out of bounds {} < {}", id, n).data());
        const float* y = data + (id * dim);
        simsimd_cos_f32(query, y, dim, result);
    }

    inline void computeDistance(vector_id_t src, vector_id_t dest, double* result) override {
        KU_ASSERT_MSG(src < n && dest < n, "Index out of bounds");
        const float* x = data + (src * dim);
        const float* y = data + (dest * dim);
        simsimd_cos_f32(x, y, dim, result);
    }

    inline void computeDistance(const float* dest, double* result) override {
        simsimd_cos_f32(query, dest, dim, result);
    }

    inline void batchComputeDistances(const float* dest, int size, double* results) override {
        for (int i = 0; i < size; i++) {
            computeDistance(dest + (i * dim), &results[i]);
        }
    }

    inline void batchComputeDistances(vector_id_t* ids, double* results, int size) override {
        for (int i = 0; i < size; i++) {
            computeDistance(ids[i], &results[i]);
        }
    }

    inline void setQuery(const float* q) override { this->query = q; }

    inline std::unique_ptr<DistanceComputer> copy() override {
        return std::make_unique<CosineDistanceComputer>(data, dim, n);
    }

private:
    const float* data;
    int dim;
    int n;

    const float* query;
};



} // namespace common
} // namespace kuzu
