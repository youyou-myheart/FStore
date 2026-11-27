#pragma once
#include <initializer_list>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace unit {
inline void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
template<class Error, class Operation> Error expect(Operation&& operation) {
    try { operation(); } catch (const Error& error) { return error; }
    throw std::runtime_error("expected exception was not thrown");
}
struct Case { std::string_view name; void (*body)(); };
inline int run(std::initializer_list<Case> cases) {
    int failed = 0;
    for (const auto& test : cases) {
        try { test.body(); std::cout << "PASS " << test.name << '\n'; }
        catch (const std::exception& error) { ++failed; std::cerr << "FAIL " << test.name << ": " << error.what() << '\n'; }
    }
    return failed ? 1 : 0;
}
}
