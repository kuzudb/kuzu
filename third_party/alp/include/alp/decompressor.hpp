#ifndef ALP_DECOMPRESSOR_HPP
#define ALP_DECOMPRESSOR_HPP

#include "alp/decode.hpp"
#include "alp/storer.hpp"
#include "alp/utils.hpp"
#include "fastlanes/unffor.hpp"

namespace alp {

/*
 * API Decompressor
 */
template <class T>
struct AlpDecompressor {

	using EXACT_TYPE = typename FloatingToExact<T>::type;

	state             stt;
	storer::MemReader reader;

	size_t out_offset = 0;

	T        exceptions[config::VECTOR_SIZE];
	int64_t  encoded_integers[config::VECTOR_SIZE];
	int64_t  alp_encoded_array[config::VECTOR_SIZE];
	uint16_t exceptions_rd[config::VECTOR_SIZE];
	uint16_t exceptions_position[config::VECTOR_SIZE];

	// 'right' & 'left' refer to the respective parts of the floating numbers after splitting
	uint64_t   alp_bp_size;
	uint64_t   left_bp_size;
	uint64_t   right_bp_size;
	EXACT_TYPE right_parts[config::VECTOR_SIZE];
	EXACT_TYPE right_parts_encoded[config::VECTOR_SIZE];
	uint16_t   left_parts_encoded[config::VECTOR_SIZE];
	uint16_t   left_parts[config::VECTOR_SIZE];

	EXACT_TYPE right_for_base = 0; // Always 0

	AlpDecompressor() {

	};

	void load_rd_metadata() {
		reader.read(&stt.right_bit_width, sizeof(stt.right_bit_width));
		reader.read(&stt.left_bit_width, sizeof(stt.left_bit_width));
		left_bp_size  = AlpApiUtils<T>::get_size_after_bitpacking(stt.left_bit_width);
		right_bp_size = AlpApiUtils<T>::get_size_after_bitpacking(stt.right_bit_width);

		reader.read(&stt.actual_dictionary_size, sizeof(stt.actual_dictionary_size));
		uint8_t actual_dictionary_size_bytes = stt.actual_dictionary_size * DICTIONARY_ELEMENT_SIZE_BYTES;

		reader.read(stt.left_parts_dict, actual_dictionary_size_bytes);
	}

	void load_alprd_vector() {
		reader.read(&stt.exceptions_count, sizeof(stt.exceptions_count));
		reader.read(left_parts_encoded, left_bp_size);
		reader.read(right_parts_encoded, right_bp_size);
		if (stt.exceptions_count) {
			reader.read(exceptions_rd, RD_EXCEPTION_SIZE_BYTES * stt.exceptions_count);
			reader.read(exceptions_position, RD_EXCEPTION_POSITION_SIZE_BYTES * stt.exceptions_count);
		}
	}

	void load_alp_vector() {
		reader.read(&stt.exp, sizeof(stt.exp));
		reader.read(&stt.fac, sizeof(stt.fac));
		reader.read(&stt.exceptions_count, sizeof(stt.exceptions_count));
		reader.read(&stt.for_base, sizeof(stt.for_base));
		reader.read(&stt.bit_width, sizeof(stt.bit_width));

		if (stt.bit_width > 0) {
			alp_bp_size = AlpApiUtils<T>::get_size_after_bitpacking(stt.bit_width);
			reader.read(alp_encoded_array, alp_bp_size);
		}

		if (stt.exceptions_count > 0) {
			reader.read(exceptions, Constants<T>::EXCEPTION_SIZE_BYTES * stt.exceptions_count);
			reader.read(exceptions_position, EXCEPTION_POSITION_SIZE_BYTES * stt.exceptions_count);
		}
	}

	void decompress_vector(T* out) {
		if (stt.scheme == SCHEME::ALP_RD) {
			unffor::unffor(right_parts_encoded, right_parts, stt.right_bit_width, &right_for_base);
			unffor::unffor(left_parts_encoded, left_parts, stt.left_bit_width, &stt.left_for_base);
			AlpRD<T>::decode((out + out_offset),
			                 right_parts,
			                 left_parts,
			                 exceptions_rd,
			                 exceptions_position,
			                 &stt.exceptions_count,
			                 stt);
		} else {
			unffor::unffor(alp_encoded_array, encoded_integers, stt.bit_width, &stt.for_base);
			AlpDecode<T>::decode(encoded_integers, stt.fac, stt.exp, (out + out_offset));
			AlpDecode<T>::patch_exceptions((out + out_offset), exceptions, exceptions_position, &stt.exceptions_count);
		}
	}

	void load_vector() {
		if (stt.scheme == SCHEME::ALP_RD) {
			load_alprd_vector();
		} else {
			load_alp_vector();
		}
	}

	SCHEME load_rowgroup_metadata() {
		uint8_t scheme_id;
		reader.read(&scheme_id, sizeof(scheme_id));

		SCHEME used_scheme = SCHEME(scheme_id);
		if (used_scheme == SCHEME::ALP_RD) { load_rd_metadata(); }

		return used_scheme;
	}

	void decompress(uint8_t* in, size_t values_count, T* out) {
		reader                    = storer::MemReader(in);
		size_t rouwgroup_count    = AlpApiUtils<T>::get_rowgroup_count(values_count);
		size_t left_to_decompress = values_count;
		for (size_t current_rowgroup = 0; current_rowgroup < rouwgroup_count; current_rowgroup++) {
			/*
			 * Rowgroup level
			 */
			stt.scheme = load_rowgroup_metadata();

			size_t values_left_in_rowgroup = std::min(config::ROWGROUP_SIZE, left_to_decompress);
			size_t vectors_in_rowgroup     = AlpApiUtils<T>::get_complete_vector_count(values_left_in_rowgroup);

			for (size_t vector_idx = 0; vector_idx < vectors_in_rowgroup; vector_idx++) {
				/*
				 * Vector level
				 */
				size_t next_vector_count = std::min(config::VECTOR_SIZE, left_to_decompress);
				load_vector();
				decompress_vector(out);
				out_offset += next_vector_count;
				left_to_decompress -= next_vector_count;
			}
		}
		if (left_to_decompress) {
			load_vector();
			decompress_vector(out);
		}
	};
};

} // namespace alp

#endif