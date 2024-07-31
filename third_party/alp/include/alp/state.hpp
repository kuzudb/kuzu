#ifndef ALP_STATE_HPP
#define ALP_STATE_HPP

#include "alp/common.hpp"
#include "alp/config.hpp"
#include "alp/constants.hpp"
#include <cstdint>
#include <unordered_map>

namespace alp {
struct state {
	SCHEME   scheme {SCHEME::ALP};
	uint16_t vector_size {config::VECTOR_SIZE};
	uint32_t exceptions_count {0};
	size_t   sampled_values_n {0};

	// ALP
	uint16_t                         k_combinations {5};
	std::vector<std::pair<int, int>> best_k_combinations;
	uint8_t                          exp;
	uint8_t                          fac;
	bw_t                             bit_width;
	int64_t                          for_base;

	// ALP RD
	bw_t                                   right_bit_width {0};
	bw_t                                   left_bit_width {0};
	uint64_t                               right_for_base {0}; // Always 0
	uint16_t                               left_for_base {0};  // Always 0
	uint16_t                               left_parts_dict[config::MAX_RD_DICTIONARY_SIZE];
	uint8_t                                actual_dictionary_size;
	uint32_t                               actual_dictionary_size_bytes;
	std::unordered_map<uint16_t, uint16_t> left_parts_dict_map;
};
} // namespace alp

#endif
