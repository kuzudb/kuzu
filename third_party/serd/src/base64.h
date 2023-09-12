// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#ifndef SERD_SRC_BASE64_H
#define SERD_SRC_BASE64_H

#include "serd.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
   Return the number of bytes required to encode `size` bytes in base64.

   @param size The number of input (binary) bytes to encode.
   @param wrap_lines Wrap lines at 76 characters to conform to RFC 2045.
   @return The length of the base64 encoding, excluding null terminator.
*/
SERD_CONST_FUNC size_t
serd_base64_get_length(size_t size, bool wrap_lines);

/**
   Encode `size` bytes of `buf` into `str`, which must be large enough.

   @param str Output string buffer.
   @param buf Input binary data.
   @param size Number of bytes to encode from `buf`.
   @param wrap_lines Wrap lines at 76 characters to conform to RFC 2045.
   @return True iff `str` contains newlines.
*/
bool
serd_base64_encode(uint8_t* str, const void* buf, size_t size, bool wrap_lines);

#endif // SERD_SRC_BASE64_H
