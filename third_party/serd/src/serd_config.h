// Copyright 2021-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

/*
  Configuration header that defines reasonable defaults at compile-time.

  This allows configuration from the command-line (usually by the build system)
  while still allowing the code to compile "as-is" with reasonable default
  features on supported platforms.

  This system is designed so that, ideally, no command-line or build-system
  configuration is needed, but automatic feature detection can be disabled or
  overridden for maximum control.  It should never be necessary to edit the
  source code to achieve a given configuration.

  Usage:

  - By default, features are enabled if they can be detected or assumed to be
    available from the build environment, unless `SERD_NO_DEFAULT_CONFIG` is
    defined, which disables everything by default.

  - If a symbol like `HAVE_SOMETHING` is defined to non-zero, then the
    "something" feature is assumed to be available.

  Code rules:

  - To check for a feature, this header must be included, and the symbol
    `USE_SOMETHING` used as a boolean in an `#if` expression.

  - None of the other configuration symbols described here may be used
    directly.  In particular, this header should be the only place in the
    source code that touches `HAVE` symbols.
*/

#ifndef SERD_SRC_SERD_CONFIG_H
#define SERD_SRC_SERD_CONFIG_H

// Define version unconditionally so a warning will catch a mismatch
#define SERD_VERSION "0.31.5"

#if !defined(SERD_NO_DEFAULT_CONFIG)

// We need unistd.h to check _POSIX_VERSION
#  ifdef __has_include
#    if __has_include(<unistd.h>)
#      include <unistd.h>
#    endif
#  elif defined(__unix__)
#    include <unistd.h>
#  endif

// Define SERD_POSIX_VERSION unconditionally for convenience
#  if defined(_POSIX_VERSION)
#    define SERD_POSIX_VERSION _POSIX_VERSION
#  else
#    define SERD_POSIX_VERSION 0
#  endif

// POSIX.1-2001: fileno()
#  ifndef HAVE_FILENO
#    if SERD_POSIX_VERSION >= 200112L || defined(_WIN32)
#      define HAVE_FILENO 1
#    endif
#  endif

// POSIX.1-2001: posix_fadvise()
#  ifndef HAVE_POSIX_FADVISE
#    if SERD__POSIX_VERSION >= 200112L && !defined(__APPLE__)
#      define HAVE_POSIX_FADVISE 1
#    endif
#  endif

// POSIX.1-2001: posix_memalign()
#  ifndef HAVE_POSIX_MEMALIGN
#    if SERD_POSIX_VERSION >= 200112L
#      define HAVE_POSIX_MEMALIGN 1
#    endif
#  endif

#endif // !defined(SERD_NO_DEFAULT_CONFIG)

/*
  Unconditionally define symbols like USE_SOMETHING based on HAVE_SOMETHING, if
  it's defined.  The code checks these using #if (not #ifdef), so there will be
  a warning if it checks for an unknown feature or doesn't include this header.
  This header should be the only file in the source code that touches symbols
  like HAVE_SOMETHING.
*/

#if defined(HAVE_FILENO) && HAVE_FILENO
#  define USE_FILENO 1
#else
#  define USE_FILENO 0
#endif

#if defined(HAVE_POSIX_FADVISE) && HAVE_POSIX_FADVISE
#  define USE_POSIX_FADVISE 1
#else
#  define USE_POSIX_FADVISE 0
#endif

#if defined(HAVE_POSIX_MEMALIGN) && HAVE_POSIX_MEMALIGN
#  define USE_POSIX_MEMALIGN 1
#else
#  define USE_POSIX_MEMALIGN 0
#endif

#endif // SERD_SRC_SERD_CONFIG_H
