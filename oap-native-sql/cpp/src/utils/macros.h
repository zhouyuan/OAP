#pragma once

#define TIME_MICRO_OR_RAISE(time, expr)                                                 \
  do {                                                                                  \
    auto start = std::chrono::steady_clock::now();                                      \
    auto __s = (expr);                                                                  \
    if (!__s.ok()) {                                                                    \
      return __s;                                                                       \
    }                                                                                   \
    auto end = std::chrono::steady_clock::now();                                        \
    time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(); \
  } while (false);

#define TIME_MICRO_OR_THROW(time, expr)                                                 \
  do {                                                                                  \
    auto start = std::chrono::steady_clock::now();                                      \
    auto __s = (expr);                                                                  \
    if (!__s.ok()) {                                                                    \
      throw std::runtime_error(__s.message());                                          \
    }                                                                                   \
    auto end = std::chrono::steady_clock::now();                                        \
    time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(); \
  } while (false);

#define TIME_TO_STRING(time) \
  (time > 10000 ? time / 1000 : time) << (time > 10000 ? " ms" : " us")
