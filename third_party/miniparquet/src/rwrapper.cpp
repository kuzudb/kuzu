
// motherfucker
#undef error
#undef length

#include <iostream>
#include <cmath>

#include "miniparquet.h"
#undef ERROR
#include <Rdefines.h>
#undef nrows

using namespace miniparquet;
using namespace std;

// surely they are joking
constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;
constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

static int64_t impala_timestamp_to_nanoseconds(const Int96& impala_timestamp) {
	int64_t days_since_epoch = impala_timestamp.value[2]
			- kJulianToUnixEpochDays;

	int64_t nanoseconds;
	memcpy(&nanoseconds, impala_timestamp.value, sizeof(nanoseconds));
	return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

extern "C" {

SEXP miniparquet_read(SEXP filesxp) {

	if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
		Rf_error("miniparquet_read: Need single filename parameter");
	}

	try {
		// parse the query and transform it into a set of statements

		char *fname = (char *) CHAR(STRING_ELT(filesxp, 0));

		ParquetFile f(fname);

		// allocate vectors

		auto ncols = f.columns.size();
		auto nrows = f.nrow;

		SEXP retlist = PROTECT(NEW_LIST(ncols));
		if (!retlist) {
			UNPROTECT(1); // retlist
			Rf_error("miniparquet_read: Memory allocation failed");
		}
		SEXP names = PROTECT(NEW_STRING(ncols));
		if (!names) {
			UNPROTECT(2); // retlist, names
			Rf_error("miniparquet_read: Memory allocation failed");
		}
		SET_NAMES(retlist, names);
		UNPROTECT(1); // names

		for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
			SEXP varname = PROTECT(
					mkCharCE(f.columns[col_idx]->name.c_str(), CE_UTF8));
			if (!varname) {
				UNPROTECT(2); // varname, retlist
				Rf_error("miniparquet_read: Memory allocation failed");
			}
			SET_STRING_ELT(names, col_idx, varname);
			UNPROTECT(1); // varname

			SEXP varvalue = NULL;
			switch (f.columns[col_idx]->type) {
			case parquet::format::Type::BOOLEAN:
				varvalue = PROTECT(NEW_LOGICAL(nrows));
				break;
			case parquet::format::Type::INT32:
				varvalue = PROTECT(NEW_INTEGER(nrows));
				break;
			case parquet::format::Type::INT64:
			case parquet::format::Type::DOUBLE:
			case parquet::format::Type::FLOAT:
				varvalue = PROTECT(NEW_NUMERIC(nrows));
				break;
			case parquet::format::Type::INT96: {
				varvalue = PROTECT(NEW_NUMERIC(nrows));
				SEXP cl = PROTECT(NEW_STRING(2));
				SET_STRING_ELT(cl, 0, PROTECT(mkChar("POSIXct")));
				SET_STRING_ELT(cl, 1, PROTECT(mkChar("POSIXt")));
				SET_CLASS(varvalue, cl);
				setAttrib(varvalue, install("tzone"), PROTECT(mkString("UTC")));
				UNPROTECT(4);
				break;
			}
			case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: { // oof
				auto& s_ele = f.columns[col_idx]->schema_element;
				if (!s_ele->__isset.converted_type) {
					throw runtime_error("Missing FLBA type");
				}
				switch (s_ele->converted_type) {
				case parquet::format::ConvertedType::DECIMAL:
					varvalue = PROTECT(NEW_NUMERIC(nrows));
					break;
				default:
					UNPROTECT(1); // retlist
					auto it =
							parquet::format::_ConvertedType_VALUES_TO_NAMES.find(
									s_ele->converted_type);
					Rf_error("miniparquet_read: Unknown FLBA type %s",
							it->second);
				}
				break;
			}
			case parquet::format::Type::BYTE_ARRAY:
				varvalue = PROTECT(NEW_STRING(nrows));
				break;
			default:
				UNPROTECT(1); // retlist
				auto it = parquet::format::_Type_VALUES_TO_NAMES.find(
						f.columns[col_idx]->type);
				Rf_error("miniparquet_read: Unknown column type %s",
						it->second); // unlikely
			}
			if (!varvalue) {
				UNPROTECT(2); // varvalue, retlist
				Rf_error("miniparquet_read: Memory allocation failed");
			}
			SET_VECTOR_ELT(retlist, col_idx, varvalue);
			UNPROTECT(1); /* varvalue */
		}

		// at this point retlist is fully allocated and the only protected SEXP

		ResultChunk rc;
		ScanState s;

		f.initialize_result(rc);
		uint64_t dest_offset = 0;

		while (f.scan(s, rc)) {
			for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
				auto& col = rc.cols[col_idx];
				SEXP dest = VECTOR_ELT(retlist, col_idx);

				for (uint64_t row_idx = 0; row_idx < rc.nrows; row_idx++) {
					if (!col.defined.ptr[row_idx]) {

						// NULLs
						switch (f.columns[col_idx]->type) {
						case parquet::format::Type::BOOLEAN:
							LOGICAL_POINTER(dest)[row_idx + dest_offset] =
									NA_LOGICAL;
							break;
						case parquet::format::Type::INT32:
							INTEGER_POINTER(dest)[row_idx + dest_offset] =
									NA_INTEGER;
							break;
						case parquet::format::Type::INT64:
						case parquet::format::Type::DOUBLE:
						case parquet::format::Type::FLOAT:
						case parquet::format::Type::INT96:
							NUMERIC_POINTER(dest)[row_idx + dest_offset] =
									NA_REAL;
							break;
						case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: { // oof, TODO duplication above
							auto& s_ele = f.columns[col_idx]->schema_element;
							if (!s_ele->__isset.converted_type) {
								throw runtime_error("Missing FLBA type");
							}
							switch (s_ele->converted_type) {
							case parquet::format::ConvertedType::DECIMAL:
								NUMERIC_POINTER(dest)[row_idx + dest_offset] =
										NA_REAL;
								break;
							default:
								UNPROTECT(1); // retlist
								auto it =
										parquet::format::_ConvertedType_VALUES_TO_NAMES.find(
												s_ele->converted_type);
								Rf_error(
										"miniparquet_read: Unknown FLBA type %s",
										it->second);
							}
							break;
						}
						case parquet::format::Type::BYTE_ARRAY:
							SET_STRING_ELT(dest, row_idx + dest_offset,
									NA_STRING);

							break;

						default: {
							UNPROTECT(1); // retlist
							auto it =
									parquet::format::_Type_VALUES_TO_NAMES.find(
											f.columns[col_idx]->type);
							Rf_error("miniparquet_read: Unknown column type %s",
									it->second); // unlikely
						}
						}
						continue;
					}

					switch (f.columns[col_idx]->type) {
					case parquet::format::Type::BOOLEAN:
						LOGICAL_POINTER(dest)[row_idx + dest_offset] =
								((bool*) col.data.ptr)[row_idx];
						break;
					case parquet::format::Type::INT32:
						INTEGER_POINTER(dest)[row_idx + dest_offset] =
								((int32_t*) col.data.ptr)[row_idx];
						break;
					case parquet::format::Type::INT64:
						NUMERIC_POINTER(dest)[row_idx + dest_offset] =
								(double) ((int64_t*) col.data.ptr)[row_idx];
						break;
					case parquet::format::Type::DOUBLE:
						NUMERIC_POINTER(dest)[row_idx + dest_offset] =
								((double*) col.data.ptr)[row_idx];
						break;
					case parquet::format::Type::FLOAT:
						NUMERIC_POINTER(dest)[row_idx + dest_offset] =
								(double) ((float*) col.data.ptr)[row_idx];
						break;
					case parquet::format::Type::INT96:
						NUMERIC_POINTER(dest)[row_idx + dest_offset] =
								impala_timestamp_to_nanoseconds(
										((Int96*) col.data.ptr)[row_idx])
										/ 1000000000;
						break;

					case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: { // oof, TODO clusterfuck
						auto& s_ele = f.columns[col_idx]->schema_element;
						if (!s_ele->__isset.converted_type) {
							throw runtime_error("Missing FLBA type");
						}
						switch (s_ele->converted_type) {
						case parquet::format::ConvertedType::DECIMAL:

						{

							// this is a giant clusterfuck
							auto type_len = s_ele->type_length;
							auto bytes =((char**)col.data.ptr)[row_idx];
							int64_t val = 0;
							for (auto i = 0; i < type_len; i++) {
								val = val << ((type_len - i) * 8)
										| (uint8_t) bytes[i];
							}

							NUMERIC_POINTER(dest)[row_idx + dest_offset] = val
									/ pow(10.0, s_ele->scale);

						}

							break;
						default:
							UNPROTECT(1); // retlist
							auto it =
									parquet::format::_ConvertedType_VALUES_TO_NAMES.find(
											s_ele->converted_type);
							Rf_error("miniparquet_read: Unknown FLBA type %s",
									it->second);
						}
						break;
					}

					case parquet::format::Type::BYTE_ARRAY:
						SET_STRING_ELT(dest, row_idx + dest_offset,
								mkCharCE(
										((char**)col.data.ptr)[row_idx],
										CE_UTF8));
						break;

					default: {
						auto it = parquet::format::_Type_VALUES_TO_NAMES.find(
								f.columns[col_idx]->type);
						UNPROTECT(1); // retlist
						Rf_error("miniparquet_read: Unknown column type %s",
								it->second); // unlikely
					}
					}

				}

			}
			dest_offset += rc.nrows;

		}
		assert(dest_offset == nrows);
		UNPROTECT(1); // retlist
		return retlist;


	} catch (std::exception &ex) {
		Rf_error(ex.what());
		// TODO this may leak
	}

}

// R native routine registration
#define CALLDEF(name, n)                                                                                               \
	{ #name, (DL_FUNC)&name, n }
static const R_CallMethodDef R_CallDef[] = { CALLDEF(miniparquet_read, 1),

{ NULL, NULL, 0 } };

void R_init_miniparquet(DllInfo *dll) {
	R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
	R_useDynamicSymbols(dll, FALSE);
}
}
