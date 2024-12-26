#pragma once

#include <vector>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include <simsimd/simsimd.h>

using namespace kuzu::common;

namespace kuzu {
    namespace storage {
        enum DistanceType {
            L2_SQ,
        };

        template<typename T1, typename T2>
        struct QuantizedDistanceComputer {
            int dim;

            explicit QuantizedDistanceComputer(int dim) : dim(dim) {}

            /**
             * Compute the distance between two vectors.
             * @param x the first vector
             * @param y the second vector
             * @param result the distance
             */
            virtual void compute_distance(const T1 *x, const T2 *y, double *result) = 0;

            virtual ~QuantizedDistanceComputer() = default;
        };

        // Serial encoding functions
        inline uint8_t encode_serial(const float x, const float vmin, const float vdiff,
                                     float scalar_range) {
            auto val = std::min(std::max(x - vmin, 0.0f), vdiff);
            return std::min(int((val / vdiff) * scalar_range), (int) scalar_range - 1);
        }

        inline float compute_precomputed_value_serial(const uint8_t code, const float alpha, const float beta) {
            return code * alpha * beta;
        }

        inline void encode_serial_8bit(const float *data, uint8_t *codes, int dim, const float *vmin,
                                       const float *vdiff, const float *alpha, const float *beta, float beta_sqr) {
            float precompute_value = beta_sqr;
            for (size_t j = 0; j < dim; j++) {
                codes[j] = encode_serial(data[j], vmin[j], vdiff[j], 256.0f);
                precompute_value += compute_precomputed_value_serial(codes[j], alpha[j], beta[j]);
            }
            // Store precomputed values
            *reinterpret_cast<float *>(codes + dim) = precompute_value;
        }

        inline float decode_serial(const uint8_t code, const float alpha, const float beta) {
            return alpha * code + beta;
        }

        inline void compute_asym_l2sq_serial_8bit(const float *x, const uint8_t *y, double *result, size_t dim,
                                                  const float *alpha, const float *beta) {
            double res = 0;
            for (size_t i = 0; i < dim; i++) {
                auto code = decode_serial(y[i], alpha[i], beta[i]);
                auto xc = (x[i] - code);
                res += xc * xc;
            }
            *result = res;
        }

        inline void compute_sym_l2sq_serial_8bit(const uint8_t *x, const uint8_t *y, double *result, size_t dim,
                                                 const float *alphaSqr) {
            double res = 0;
            for (size_t i = 0; i < dim; i++) {
                int xy = x[i] - y[i];
                res += xy * xy * alphaSqr[i];
            }
            *result = res;
        }

#if _SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_HASWELL
#pragma GCC push_options
#pragma GCC target("avx2", "fma")
#pragma clang attribute push(__attribute__((target("avx2,fma"))), apply_to = function)

        inline __m128i encode_haswell_(const float *x, size_t i, const float *vmin, const float *vdiff,
                                       const float scalar_range) {
            __m256 scalar_range_vec = _mm256_set1_ps(scalar_range);
            __m256 vmin_vec = _mm256_loadu_ps(vmin + i);
            __m256 vdiff_vec = _mm256_loadu_ps(vdiff + i);
            __m256 x_vec = _mm256_loadu_ps(x + i);

            // Clamp to [vmin, vmin + vdiff]
            __m256 val = _mm256_min_ps(_mm256_max_ps(_mm256_sub_ps(x_vec, vmin_vec), _mm256_setzero_ps()), vdiff_vec);


            // Scale to [0, 1]
            __m256 x_scaled = _mm256_div_ps(val, vdiff_vec);

            // Scale to [0, scalar_range) => x * scalar_range
            __m256i ci = _mm256_cvtps_epi32(_mm256_mul_ps(x_scaled, scalar_range_vec));

            // Pack and clamp to [0, 255]
            __m128i ci_low = _mm256_extracti128_si256(ci, 0);
            __m128i ci_high = _mm256_extracti128_si256(ci, 1);
            __m128i packed16 = _mm_packus_epi32(ci_low, ci_high);

            return packed16;
        }

        inline __m128i encode_haswell(const float *x, size_t i, const float *vmin, const float *vdiff,
                                      const float scalar_range) {
            __m128i ci_low = encode_haswell_(x, i, vmin, vdiff, scalar_range);
            __m128i ci_high = encode_haswell_(x, i + 8, vmin, vdiff, scalar_range);
            return _mm_packus_epi16(ci_low, ci_high);
        }

        inline __m256 calc_precomputed_values_haswell(__m128i ci_vec, size_t i, const float *alpha, const float *beta) {
            // Load and extend ci_vec data
            __m256i ci_vec32 = _mm256_cvtepu8_epi32(ci_vec);          // Extend 8-bit to 32-bit integers
            __m256 ci_vec_f32 = _mm256_cvtepi32_ps(ci_vec32);          // Convert to float

            // Load alpha and beta
            __m256 alpha_vec = _mm256_loadu_ps(alpha + i);
            __m256 beta_vec = _mm256_loadu_ps(beta + i);

            // Calculate precomputed values
            __m256 precompute_values = _mm256_mul_ps(_mm256_mul_ps(ci_vec_f32, alpha_vec), beta_vec);

            return precompute_values;
        }

        inline void encode_haswell_8bit(const float *data, uint8_t *codes, int dim, const float *vmin,
                                        const float *vdiff, const float *alpha, const float *beta) {
            int i = 0;
            __m256 precompute_values = _mm256_setzero_ps();
            for (; i + 16 <= dim; i += 16) {
                __m128i ci_vec = encode_haswell(data, i, vmin, vdiff, 256.0f);
                precompute_values = _mm256_add_ps(calc_precomputed_values_haswell(ci_vec, i, alpha, beta),
                                                  precompute_values);
                _mm_storeu_si128((__m128i *) (codes + i), ci_vec);
            }
            __m256 temp = _mm256_hadd_ps(precompute_values, precompute_values);
            __m128 sum128 = _mm_add_ps(_mm256_castps256_ps128(temp), _mm256_extractf128_ps(temp, 1));
            sum128 = _mm_hadd_ps(sum128, sum128);
            sum128 = _mm_hadd_ps(sum128, sum128);

            auto precompute_value = _mm_cvtss_f32(sum128);
            // Handle the remaining
            for (; i < dim; i++) {
                codes[i] = encode_serial(data[i], vmin[i], vdiff[i], 256.0f);
                precompute_value += compute_precomputed_value_serial(codes[i], alpha[i], beta[i]);
            }
            // Store precomputed values
            *reinterpret_cast<float *>(codes + dim) = precompute_value;
        }

#pragma clang attribute pop
#pragma GCC pop_options
#endif // SIMSIMD_TARGET_HASWELL

#if SIMSIMD_TARGET_SKYLAKE
#pragma GCC push_options
#pragma GCC target("avx512f", "avx512vl", "bmi2")
#pragma clang attribute push(__attribute__((target("avx512f,avx512vl,bmi2"))), apply_to = function)
        inline __m512 decode_skylake(const __m128i ci_vec, size_t i, const float *alpha, const float *beta) {
            __m512i ci_vec32 = _mm512_cvtepu8_epi32(ci_vec);
            __m512 ci_vec_float = _mm512_cvtepi32_ps(ci_vec32);

            // Load alpha and beta
            __m512 alpha_vec = _mm512_loadu_ps(alpha + i);
            __m512 beta_vec = _mm512_loadu_ps(beta + i);

            // x = alpha * ci + beta
            __m512 x_vec = _mm512_fmadd_ps(alpha_vec, ci_vec_float, beta_vec);

            return x_vec;
        }

        inline void compute_asym_l2sq_skylake_8bit(const float *x, const uint8_t *y, double *result, size_t dim,
                                              const float *alpha, const float *beta) {
            __m512 d2_vec = _mm512_setzero();
            __m512 y_vec, x_vec, d_vec;
            size_t i = 0;
            for (; i + 16 <= dim; i += 16) {
                x_vec = _mm512_loadu_ps(x + i);
                __m128i codes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(y + i));
                y_vec = decode_skylake(codes, i, alpha, beta);
                d_vec = _mm512_sub_ps(x_vec, y_vec);
                d2_vec = _mm512_fmadd_ps(d_vec, d_vec, d2_vec);
            }
            *result = _mm512_reduce_add_ps(d2_vec);
        }

        inline void compute_sym_l2sq_skylake_8bit(const uint8_t *x, const uint8_t *y, double *result, size_t dim,
                                                  const float *alphaSqr) {
            // TODO
        }
#pragma clang attribute pop
#pragma GCC pop_options
#endif // SIMSIMD_TARGET_SKYLAKE
#endif // SIMSIMD_TARGET_X86

        class AsymmetricL2Sq : public QuantizedDistanceComputer<float, uint8_t> {
        public:
            explicit AsymmetricL2Sq(int dim, const float *alpha, const float *beta)
                    : QuantizedDistanceComputer(dim), alpha(alpha), beta(beta) {};

            ~AsymmetricL2Sq() = default;

            inline void compute_distance(const float *x, const uint8_t *y, double *result) override {
#if SIMSIMD_TARGET_SKYLAKE
                compute_asym_l2sq_skylake_8bit(x, y, result, dim, alpha, beta);
#else
                compute_asym_l2sq_serial_8bit(x, y, result, dim, alpha, beta);
#endif
            }

        private:
            const float *alpha;
            const float *beta;
        };

        class SymmetricL2Sq : public QuantizedDistanceComputer<uint8_t, uint8_t> {
        public:
            explicit SymmetricL2Sq(int dim, const float *alphaSqr)
                    : QuantizedDistanceComputer(dim), alphaSqr(alphaSqr) {};

            ~SymmetricL2Sq() = default;

            inline void compute_distance(const uint8_t *x, const uint8_t *y, double *result) override {
#if SIMSIMD_TARGET_SKYLAKE
                compute_sym_l2sq_skylake_8bit(x, y, result, dim, alphaSqr);
#else
                compute_sym_l2sq_serial_8bit(x, y, result, dim, alphaSqr);
#endif
            }

        private:
            const float *alphaSqr;
        };

        inline void determine_smallest_breakpoint(const float *data, size_t n, int dim, float *vmin,
                                                  float *vdiff, int num_bins, float break_point_data_ratio) {
            // Use histogram to determine the smallest break point.
            // We will use 256 bins.
            std::vector<std::vector<uint64_t>> histogram(dim);
            for (size_t i = 0; i < dim; i++) {
                histogram[i].resize(num_bins, 0);
            }

            for (size_t i = 0; i < n; i++) {
                for (size_t j = 0; j < dim; j++) {
                    // TODO: This should be using simd instruction.
                    // Determine the bin using vmin and vdiff.
                    auto bin = static_cast<uint64_t>(((data[i * dim + j] - vmin[j]) / vdiff[j]) * (float) num_bins);
                    bin = std::min((int) bin, (int) num_bins - 1);
                    histogram[j][bin]++;
                }
            }

            // Now we have to find the smallest which contains at-least 70% of n of the data.
            // Find the smallest bin range that contains at least 70% of the data
            auto threshold = n * break_point_data_ratio;
            for (size_t i = 0; i < dim; i++) {
                size_t start_bin = 0;
                size_t end_bin = 0;
                size_t min_bin_size = num_bins;
                size_t sum = 0;
                size_t left = 0;

                // Sliding window approach to find the smallest range
                for (size_t right = 0; right < num_bins; right++) {
                    sum += histogram[i][right];

                    // Shrink the window from the left if the threshold is met
                    while (sum >= threshold) {
                        if (right - left < min_bin_size) {
                            min_bin_size = right - left;
                            start_bin = left;
                            end_bin = right;
                        }
                        sum -= histogram[i][left];
                        left++;
                    }
                }
                vmin[i] = vmin[i] + (float) start_bin / (float) num_bins * vdiff[i];
                vdiff[i] = (float) (end_bin - start_bin) / (float) num_bins * vdiff[i];
            }
        }

        // Todo: Use SIMD instructions for this
        inline void scalar_batch_train(const float *x, size_t n, int dim, float scalar_range, int hist_num_bins,
                                       float break_point_data_ratio, float *vmin, float *vdiff, float *alpha,
                                       float *beta, float *alphaSqr, float *betaSqr) {
            for (size_t i = 0; i < n; i++) {
                for (size_t j = 0; j < dim; j++) {
                    vmin[j] = std::min(vmin[j], x[i * dim + j]);
                    vdiff[j] = std::max(vdiff[j], x[i * dim + j]);
                }
            }

            for (size_t i = 0; i < dim; i++) {
                vdiff[i] -= vmin[i];
            }

//            determine_smallest_breakpoint(x, n, dim, vmin, vdiff, hist_num_bins, break_point_data_ratio);
            // Precompute alpha and gamma, as well as their squares
            for (size_t i = 0; i < dim; i++) {
                alpha[i] = vdiff[i] / scalar_range;
                beta[i] = (0.5f * alpha[i]) + vmin[i];
                alphaSqr[i] = alpha[i] * alpha[i];
                betaSqr[i] = beta[i] * beta[i];
            }
        }

        class SQ8Bit {
            static constexpr size_t HISTOGRAM_NUM_BINS = 512;
        public:
            explicit SQ8Bit(int dim, float break_point_data_ratio = 0.99f)
                    : dim(dim), codeSize(dim + 4), break_point_data_ratio(break_point_data_ratio) {
                vmin = new float[dim];
                vdiff = new float[dim];
                for (size_t i = 0; i < dim; i++) {
                    vmin[i] = std::numeric_limits<float>::max();
                    vdiff[i] = std::numeric_limits<float>::lowest();
                }
                alpha = new float[dim];
                beta = new float[dim];
                alphaSqr = new float[dim];
                betaSqr = new float[dim];
            }

            SQ8Bit(const SQ8Bit &other) : dim(other.dim), codeSize(dim + 4),
                                          break_point_data_ratio(other.break_point_data_ratio) {
                vmin = new float[dim];
                vdiff = new float[dim];
                alpha = new float[dim];
                beta = new float[dim];
                alphaSqr = new float[dim];
                betaSqr = new float[dim];

                memcpy(vmin, other.vmin, dim * sizeof(float));
                memcpy(vdiff, other.vdiff, dim * sizeof(float));
                memcpy(alpha, other.alpha, dim * sizeof(float));
                memcpy(beta, other.beta, dim * sizeof(float));
                memcpy(alphaSqr, other.alphaSqr, dim * sizeof(float));
                memcpy(betaSqr, other.betaSqr, dim * sizeof(float));
            }

            inline void batch_train(const float *x, size_t n) {
                scalar_batch_train(x, n, dim, 256, HISTOGRAM_NUM_BINS, break_point_data_ratio, vmin, vdiff,
                                   alpha, beta, alphaSqr, betaSqr);
            }

            inline void encode(const float *x, uint8_t *codes, size_t n) const {
                for (size_t i = 0; i < n; i++) {
                    const float *xi = x + i * dim;
                    // We need to skip the last 4 bytes as they are precomputed values
                    uint8_t *ci = codes + i * codeSize;
#if SIMSIMD_TARGET_HASWELL
                    encode_haswell_8bit(xi, ci, dim, vmin, vdiff, alpha, beta);
#else
                    encode_serial_8bit(xi, ci, dim, vmin, vdiff, alpha, beta, 0);
#endif
                }
            }

            inline std::unique_ptr<QuantizedDistanceComputer<float, uint8_t>>
            get_asym_distance_computer(DistanceType type) const {
                switch (type) {
                    case DistanceType::L2_SQ:
                        return std::make_unique<AsymmetricL2Sq>(dim, alpha, beta);
                    default:
                        throw std::runtime_error("Unsupported distance type");
                }
            }

            inline std::unique_ptr<QuantizedDistanceComputer<uint8_t, uint8_t>>
            get_sym_distance_computer(DistanceType type) const {
                switch (type) {
                    case DistanceType::L2_SQ:
                        return std::make_unique<SymmetricL2Sq>(dim, alphaSqr);
                    default:
                        throw std::runtime_error("Unsupported distance type");
                }
            }

            inline void serialize(Serializer &serializer) const {
                serializer.write(dim);
                auto size = dim * sizeof(float);
                serializer.write(reinterpret_cast<uint8_t *>(vmin), size);
                serializer.write(reinterpret_cast<uint8_t *>(vdiff), size);
                serializer.write(reinterpret_cast<uint8_t *>(alpha), size);
                serializer.write(reinterpret_cast<uint8_t *>(beta), size);
                serializer.write(reinterpret_cast<uint8_t *>(alphaSqr), size);
                serializer.write(reinterpret_cast<uint8_t *>(betaSqr), size);
            }

            static std::unique_ptr<SQ8Bit> deserialize(Deserializer &deserializer) {
                int dim;
                deserializer.deserializeValue(dim);
                auto sq = std::make_unique<SQ8Bit>(dim);
                auto size = dim * sizeof(float);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->vmin), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->vdiff), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->alpha), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->beta), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->alphaSqr), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->betaSqr), size);
                return sq;
            }

            std::unique_ptr<SQ8Bit> copy() const { return std::make_unique<SQ8Bit>(*this); }

            ~SQ8Bit() {
                delete[] vmin;
                delete[] vdiff;
                delete[] alpha;
                delete[] beta;
                delete[] alphaSqr;
                delete[] betaSqr;
            }

        public:
            int dim; // dimension of the input vectors
            size_t codeSize; // bytes per indexed vector

        private:
            float *vmin;
            float *vdiff;

            // Precomputed values
            float *alpha;
            float *beta;
            float *alphaSqr;
            float *betaSqr;
            float break_point_data_ratio;
        };
    } // namespace storage
} // namespace kuzu
