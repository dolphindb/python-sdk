// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <type_traits>
#include <memory>

// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#ifdef _MSC_VER

#if _MSVC_LANG >= 202002L
#define DDB_CPP 20
#elif _MSVC_LANG >= 201703L
#define DDB_CPP 17
#else
#define DDB_CPP 14
#endif

#else

#if __cplusplus >= 202302L
#define DDB_CPP 23
#elif __cplusplus >= 202002L
#define DDB_CPP 20
#elif __cplusplus >= 201703L
#define DDB_CPP 17
#elif __cplusplus >= 201402L
#define DDB_CPP 14
#else
#define DDB_CPP 11
#endif

#endif

// NOLINTEND(cppcoreguidelines-macro-usage)

#ifndef _MSC_VER

#if DDB_CPP < 14
namespace std
{

template <bool B, class T = void> using enable_if_t = typename enable_if<B, T>::type;

template <bool B, class T, class F> using conditional_t = typename conditional<B, T, F>::type;

template <class T> using make_unsigned_t = typename make_unsigned<T>::type;

template <class T> using add_pointer_t = typename add_pointer<T>::type;

template<class T, class... Args>
std::enable_if_t<!std::is_array<T>::value, std::unique_ptr<T>>
make_unique(Args&&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

} // namespace std
#endif

#if DDB_CPP == 14
namespace std
{

template <class T, class U> constexpr bool is_same_v = is_same<T, U>::value;

template <class T> constexpr bool is_unsigned_v = is_unsigned<T>::value;

template <class T> constexpr bool is_integral_v = is_integral<T>::value;

template <class T> constexpr bool is_floating_point_v = is_floating_point<T>::value;

} // namespace std
#endif

#if DDB_CPP < 14
#define DDB_DEPRECATED __attribute__((deprecated))
#else
#define DDB_DEPRECATED [[deprecated]]
#endif

#else

#define DDB_DEPRECATED

#endif
