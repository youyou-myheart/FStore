#include "chunk_repair.hpp"
#include "chunk_rpc_limits.hpp"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/crc32c.h>
#include <butil/guid.h>
#include <cerrno>
#include <limits>
#include <system_error>

namespace chunkstore {
namespace wire = rpc::v1;
using namespace std::chrono_literals;

namespace {
std::uint32_t checksum(const butil::IOBuf& data) {
    std::uint32_t crc = 0;
    for (std::size_t i = 0; i < data.backing_block_num(); ++i) {
        const auto part = data.backing_block(i);
        crc = butil::crc32c::Extend(crc, part.data(), part.size());
    }
    return crc;
}

void check_io(ssize_t count, const char* operation) {
    if (count <= 0) {
        throw std::system_error(count == 0 ? EIO : errno, std::generic_category(), operation);
    }
}
}

std::uint32_t repair_block_size(const TrafficLimiter& limiter) {
    return std::clamp<std::uint64_t>(limiter.rate() / 32, 4096, max_rpc_payload);
}

TrafficLane copy_lane(wire::CopyPriority priority) {
    switch (priority) {
    case wire::NORMAL: return TrafficLane::CopyNormal;
    case wire::HIGH: return TrafficLane::CopyHigh;
    case wire::LOW: return TrafficLane::CopyLow;
    default: throw std::invalid_argument("invalid replication priority");
    }
}

RepairReceiver::RepairReceiver(ChunkStore& store)
    : store_(store), cleanup_([this](std::stop_token stop) {
        std::unique_lock lock(mutex_);
        while (!stop.stop_requested()) {
            auto deadline = std::chrono::steady_clock::time_point::max();
            for (auto& slot : transfers_) {
                if (slot && !slot->busy && slot->deadline <= std::chrono::steady_clock::now()) slot.reset();
                if (slot && !slot->busy) deadline = std::min(deadline, slot->deadline);
            }
            // New sessions may have earlier deadlines. Wake and recalculate.
            changed_.wait_until(lock, stop, deadline, [&] {
                return std::any_of(transfers_.begin(), transfers_.end(), [&](const auto& slot) {
                    return slot && !slot->busy && slot->deadline < deadline;
                });
            });
        }
    }) {}

void RepairReceiver::discard(std::string_view id) {
    std::unique_lock lock(mutex_);
    changed_.wait(lock, [&] {
        return std::none_of(transfers_.begin(), transfers_.end(), [&](const auto& slot) {
            return slot && slot->id == id && slot->busy;
        });
    });
    for (auto& slot : transfers_) {
        if (slot && slot->id == id) slot.reset();
    }
    changed_.notify_all();
}

std::uint64_t RepairReceiver::receive(const wire::RepairBlock& block, butil::IOBuf data) {
    const auto lane = copy_lane(block.priority());
    if (!block.abort()) {
        if (!is_closed(detail::wire_state(block))) {
            throw RepairConflict("repair blocks must come from a frozen or sealed chunk");
        }
        if (block.transfer_id().empty() || block.transfer_id().size() > 64 ||
            block.capacity() == 0 || block.capacity() > std::numeric_limits<std::int64_t>::max() ||
            block.total_length() > block.capacity() || block.offset() > block.total_length() ||
            data.size() > max_rpc_payload || data.size() > block.total_length() - block.offset() ||
            (data.empty() && block.total_length() != 0)) {
            throw std::invalid_argument("invalid repair block");
        }
        if (checksum(data) != block.crc32c()) {
            throw RepairConflict("repair block checksum mismatch");
        }
    }
    std::unique_lock lock(mutex_);
    auto& active = transfers_[lane_index(lane) - lane_index(TrafficLane::CopyNormal)];
    changed_.wait(lock, [&] { return !active || !active->busy; });
    const bool matches = active && active->id == block.chunk_id() &&
                         active->token == block.transfer_id();
    if (block.abort()) {
        if (matches) {
            active.reset();
            changed_.notify_all();
        }
        return 0;
    }
    if (!active) {
        if (block.offset() != 0) {
            throw RepairConflict("repair session expired; restart the transfer");
        }
        for (const auto& slot : transfers_) {
            if (slot && slot->id == block.chunk_id()) {
                throw RepairConflict("destination already receiving this chunk");
            }
        }
        active.emplace(block);
    } else if (!matches || active->next != block.offset() ||
               active->info.length != block.total_length() ||
               active->info.capacity != block.capacity() || active->epoch != block.pg_epoch() ||
               active->recovery_token != block.recovery_token() || active->info.state != detail::wire_state(block)) {
        throw RepairConflict("destination is receiving another transfer or expected a different block");
    }
    auto& transfer = *active;
    transfer.busy = true;
    lock.unlock();
    try {
        if (!transfer.file.is_valid()) transfer.file.reset(store_.open_repair_file(transfer.id, transfer.epoch, transfer.recovery_token));
        while (!data.empty()) {
            const auto count = data.pcut_into_file_descriptor(transfer.file.get(), transfer.next);
            if (count < 0 && errno == EINTR) {
                continue;
            }
            check_io(count, "write repair block");
            transfer.next += count;
        }
        const auto next = transfer.next;
        if (next == transfer.info.length) {
            store_.install_replica(transfer.id, transfer.file.get(), transfer.info, transfer.epoch, transfer.recovery_token);
        }
        lock.lock();
        if (next == transfer.info.length) {
            active.reset();
        } else {
            transfer.busy = false;
            transfer.deadline = std::chrono::steady_clock::now() + 60s;
        }
        changed_.notify_all();
        return next;
    } catch (...) {
        lock.lock();
        active.reset();
        changed_.notify_all();
        throw;
    }
}

void seal_pg(ChunkStore& store, const wire::SealPGRequest& request, wire::PGInventoryResponse& response) {
    const auto page = store.inventory().page(request.pg_id(), request.after_chunk(), request.limit() ? request.limit() : 16);
    if (!request.recovery_token() || page.token != request.recovery_token() || page.mode != PGMode::Source) {
        throw WriteUnavailable("PG is not fenced for sealing");
    }
    response.set_chunk_count(page.count); response.set_open_count(page.open_count);
    response.set_generation(page.generation); response.set_recovery_token(page.token);
    response.set_pg_epoch(page.epoch); response.set_mode(wire::PG_SOURCE);
    if (page.next) response.set_next_chunk(*page.next);
    for (const auto& chunk : page.chunks) {
        ReplicaInfo info;
        butil::ScopedFD snapshot(store.open_snapshot(chunk.id, info, page.epoch, page.token, false));
        auto* item = response.add_chunks(); item->set_chunk_id(chunk.id); detail::set_wire_state(*item, info.state);
        item->set_length(info.length); item->set_capacity(info.capacity);
        if (request.inspect_only()) {
            std::uint32_t crc = 0;
            const auto block_size = repair_block_size(store.traffic());
            for (std::uint64_t offset = 0; offset < info.length;) {
                const auto size = std::min<std::uint64_t>(block_size, info.length - offset);
                store.traffic().acquire(TrafficLane::CopyNormal, size);
                butil::IOPortal data;
                while (data.size() < size) {
                    const auto count = data.pappend_from_file_descriptor(snapshot.get(), offset + data.size(), size - data.size());
                    if (count < 0 && errno == EINTR) continue;
                    check_io(count, "inspect PG chunk");
                }
                for (std::size_t i = 0; i < data.backing_block_num(); ++i) {
                    const auto part = data.backing_block(i); crc = butil::crc32c::Extend(crc, part.data(), part.size());
                }
                offset += size;
            }
            item->set_crc32c(crc);
        } else {
            store.traffic().acquire(TrafficLane::CopyNormal, 0);
            store.seal_chunk(chunk.id, page.token, info.length);
            // A concurrent user Freeze may have won the first-close race.
            ReplicaInfo closed;
            butil::ScopedFD checked(store.open_snapshot(chunk.id, closed, page.epoch, page.token));
            detail::set_wire_state(*item, closed.state);
        }
    }
    if (!request.inspect_only()) {
        const auto current = store.inventory().page(request.pg_id(), "", 1);
        response.set_open_count(current.open_count);
        response.set_generation(current.generation);
    }
}

void push_pg(ChunkStore& store, const wire::RepairPGRequest& request, wire::RepairPGResponse& response) {
    const auto page = store.inventory().page(request.pg_id(), request.after_chunk(), request.limit() ? request.limit() : 16);
    if (!request.recovery_token() || page.token != request.recovery_token() || page.mode != PGMode::Source) {
        throw WriteUnavailable("source PG is not fenced for this recovery");
    }
    for (const auto& chunk : page.chunks) {
        if (!is_closed(chunk.state)) throw RepairConflict("all PG chunks must be frozen or sealed before copying");
        wire::RepairRequest copy; copy.set_chunk_id(chunk.id); copy.set_destination(request.destination());
        copy.set_priority(request.priority()); copy.set_recovery_token(request.recovery_token());
        wire::RepairResponse result; push_replica(store, copy, result);
        if (result.status().code() != wire::OK) { *response.mutable_status() = result.status(); return; }
        response.set_copied_chunks(response.copied_chunks() + 1);
        response.set_copied_bytes(response.copied_bytes() + result.length());
    }
    if (page.next) response.set_next_chunk(*page.next);
}

void push_replica(ChunkStore& store, const wire::RepairRequest& request,
                  wire::RepairResponse& response) {
    const auto lane = copy_lane(request.priority());
    store.traffic().acquire(lane, 0);
    ReplicaInfo info;
    butil::ScopedFD source(store.open_snapshot(request.chunk_id(), info, request.pg_epoch(), request.recovery_token()));
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.max_retry = 0;
    options.timeout_ms = 30000;
    if (request.destination().empty() || channel.Init(request.destination().c_str(), &options) != 0) {
        throw std::invalid_argument("invalid destination endpoint");
    }
    wire::ChunkService_Stub peer(&channel);
    wire::RepairBlock block;
    block.set_chunk_id(request.chunk_id());
    block.set_pg_epoch(request.pg_epoch());
    block.set_recovery_token(request.recovery_token());
    block.set_priority(request.priority());
    block.set_transfer_id(butil::GenerateGUID());
    block.set_capacity(info.capacity);
    block.set_total_length(info.length);
    detail::set_wire_state(block, info.state);
    std::uint64_t offset = 0;
    bool contacted = false;
    const auto local_block = repair_block_size(store.traffic());
    // Learn the peer's block size from its first acknowledgement.
    std::uint32_t block_size = std::min(local_block, 64u * 1024);
    try {
        do {
            const auto size = std::min<std::uint64_t>(block_size, info.length - offset);
            store.traffic().acquire(lane, size);
            butil::IOPortal data;
            while (data.size() < size) {
                const auto count = data.pappend_from_file_descriptor(source.get(),
                    offset + data.size(), size - data.size());
                if (count < 0 && errno == EINTR) {
                    continue;
                }
                check_io(count, "read repair snapshot");
            }
            block.set_offset(offset);
            block.set_crc32c(checksum(data));
            brpc::Controller call;
            call.request_attachment().swap(data);
            wire::RepairResponse ack;
            contacted = true;
            peer.ReceiveRepairBlock(&call, &block, &ack, nullptr);
            if (call.Failed()) {
                throw std::runtime_error("destination RPC: " + call.ErrorText());
            }
            if (!ack.has_status() || (ack.status().code() == wire::OK && ack.length() != offset + size)) {
                throw std::runtime_error("invalid destination acknowledgement");
            }
            if (ack.status().code() != wire::OK) {
                *response.mutable_status() = ack.status();
                throw std::runtime_error(ack.status().message());
            }
            block_size = ack.suggested_block_bytes() ? std::min(local_block, ack.suggested_block_bytes())
                                                     : local_block;
            offset += size;
        } while (offset < info.length);
        response.set_length(offset);
    } catch (const std::exception& e) {
        if (contacted) {
            brpc::Controller call;
            call.set_timeout_ms(1000);
            wire::RepairResponse ignored;
            block.set_abort(true);
            peer.ReceiveRepairBlock(&call, &block, &ignored, nullptr);
        }
        if (response.status().code() == wire::OK) {
            response.mutable_status()->set_code(wire::REPAIR_FAILED);
            response.mutable_status()->set_message(e.what());
        }
    }
}
}
