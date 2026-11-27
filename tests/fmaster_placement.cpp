#include "fmaster_placement.hpp"
#include "unit.hpp"

#include <array>
#include <map>
#include <vector>

namespace placement = chunkstore::master::placement;
namespace wire = fstore::master::v1;
using unit::check;
namespace {
wire::SpaceState state(std::initializer_list<std::pair<std::uint32_t, std::uint32_t>> ranges) {
    wire::SpaceState result;
    unsigned count = 0;
    for (const auto& [begin, end] : ranges) {
        auto* range = result.add_ranges(); range->set_begin(begin); range->set_end(end);
        count += end - begin;
    }
    result.mutable_space()->set_bound_pgs(count);
    return result;
}

void initial_cursor_boundaries() {
    const auto empty = state({});
    const auto error = unit::expect<chunkstore::master::Error>([&] { placement::initial_cursor(empty, 1); });
    check(error.code() == wire::CONFLICT, "empty Space returned wrong error");
    check(!placement::next_cursor(empty, 0) && !placement::next_cursor(empty, UINT64_MAX), "empty Space produced a cursor");
    const auto single = state({{9999999, 10000000}});
    for (const auto sequence : std::array<std::uint64_t, 4>{1, 2, 4096, chunkstore::ChunkId::max_sequence}) {
        check(placement::initial_cursor(single, sequence) == 9999999, "single-PG initial cursor overflowed or escaped ownership");
    }
    const auto fragmented = state({{100, 102}, {5000000, 5000003}, {9999999, 10000000}});
    // Fixed protocol examples also cover unequal interval sizes and last PG.
    for (const auto& [sequence, expected] : std::array<std::pair<std::uint64_t, std::uint64_t>, 7>{{
             {1, 101}, {2, 5000002}, {3, 5000000}, {5, 100}, {9, 5000001}, {10, 9999999},
             {chunkstore::ChunkId::max_sequence, 5000000}}}) {
        check(placement::initial_cursor(fragmented, sequence) == expected, "initial cursor golden example changed");
    }
}

void unequal_ranges_are_weighted_by_pg_count() {
    const auto value = state({{11, 12}, {7000000, 7000009}});
    std::map<std::uint64_t, unsigned> histogram;
    for (std::uint64_t sequence = 1; sequence <= 20000; ++sequence) {
        const auto pg = placement::initial_cursor(value, sequence);
        check(pg == 11 || (pg >= 7000000 && pg < 7000009), "initial cursor landed in a gap");
        ++histogram[pg];
    }
    check(histogram.size() == 10, "some PGs never receive small Blobs");
    for (const auto& [pg, count] : histogram) {
        (void)pg;
        check(count > 1700 && count < 2300, "selection was weighted by interval instead of individual PGs");
    }
}

void gaps_and_wrap() {
    const auto value = state({{2, 5}, {10, 12}, {9999998, 10000000}});
    for (const auto& [input, expected] : std::array<std::pair<std::uint64_t, std::uint64_t>, 12>{{
             {0, 2}, {2, 2}, {4, 4}, {5, 10}, {9, 10}, {11, 11}, {12, 9999998},
             {9999998, 9999998}, {9999999, 9999999}, {10000000, 10000002},
             {20000005, 20000010}, {30000012, 39999998}}}) {
        check(placement::next_cursor(value, input) == expected, "gap or wrap selected the wrong next cursor");
    }
    for (const auto pg : {0u, 1u, 5u, 9u, 12u, 9999997u, 10000000u}) {
        check(!placement::owns_pg(value, pg), "half-open interval includes an unbound PG");
    }
}

void uint64_exhaustion() {
    constexpr auto modulus = chunkstore::global_pg_count;
    constexpr auto last_base = UINT64_MAX - UINT64_MAX % modulus;
    constexpr auto last_pg = static_cast<std::uint32_t>(UINT64_MAX % modulus);
    const auto at_limit = state({{last_pg, last_pg + 1}});
    check(placement::next_cursor(at_limit, last_base) == UINT64_MAX, "last reachable cursor was skipped");
    check(placement::next_cursor(at_limit, UINT64_MAX) == UINT64_MAX, "inclusive cursor excluded UINT64_MAX");
    check(!placement::next_cursor(state({{last_pg + 1, last_pg + 2}}), last_base), "cursor overflow wrapped to an earlier cycle");
    const auto below = state({{last_pg - 1, last_pg}});
    check(placement::next_cursor(below, last_base) == UINT64_MAX - 1, "last partial cycle was truncated");
    check(!placement::next_cursor(below, UINT64_MAX), "exhausted sparse cursor wrapped");
    const auto full = state({{0, modulus}});
    for (const auto input : std::array<std::uint64_t, 5>{0, 1, modulus, UINT64_MAX - 1, UINT64_MAX}) {
        check(placement::next_cursor(full, input) == input, "fully bound pool skipped an available cursor");
    }
}

void cursor_wire_presence() {
    wire::AllocateChunksRequest request, decoded;
    check(!request.has_start_cursor(), "omitted cursor is not automatic");
    request.set_start_cursor(0);
    check(decoded.ParseFromString(request.SerializeAsString()) && decoded.has_start_cursor() && decoded.start_cursor() == 0,
          "explicit zero cursor lost presence on the wire");
    request.set_start_cursor(UINT64_MAX);
    check(decoded.ParseFromString(request.SerializeAsString()) && decoded.start_cursor() == UINT64_MAX, "cursor narrowed on wire");
    request.clear_start_cursor();
    check(decoded.ParseFromString(request.SerializeAsString()) && !decoded.has_start_cursor(), "clearing cursor did not restore automatic mode");
}
}
int main() {
    return unit::run({{"initial_boundaries", initial_cursor_boundaries}, {"unequal_ranges", unequal_ranges_are_weighted_by_pg_count},
        {"gaps_and_wrap", gaps_and_wrap}, {"uint64_exhaustion", uint64_exhaustion}, {"wire_presence", cursor_wire_presence}});
}
