#pragma once

#include "chunk.pb.h"
#include "chunk_store.hpp"
#include "chunk_wire_state.hpp"

#include <butil/iobuf.h>
#include <butil/files/scoped_file.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace chunkstore {

std::uint32_t repair_block_size(const TrafficLimiter& limiter);
TrafficLane copy_lane(rpc::v1::CopyPriority priority);

// Independent receiver slots keep other priorities available during a transfer.
class RepairReceiver {
public:
    explicit RepairReceiver(ChunkStore& store);
    void discard(std::string_view id);
    std::uint64_t receive(const rpc::v1::RepairBlock& block, butil::IOBuf data);
private:
    struct Transfer {
        explicit Transfer(const rpc::v1::RepairBlock& block)
            : id(block.chunk_id()), token(block.transfer_id()),
              info{block.total_length(), block.capacity(), detail::wire_state(block)},
              epoch(block.pg_epoch()), recovery_token(block.recovery_token()) {}

        std::string id;
        std::string token;
        ReplicaInfo info;
        std::uint64_t epoch, recovery_token;
        butil::ScopedFD file;
        bool busy = false; // pins this slot while its worker performs I/O
        std::uint64_t next = 0;
        std::chrono::steady_clock::time_point deadline;
    };
    ChunkStore& store_;
    std::mutex mutex_;
    std::condition_variable_any changed_;
    std::array<std::optional<Transfer>, 3> transfers_;
    std::jthread cleanup_;
};

void push_pg(ChunkStore& store, const rpc::v1::RepairPGRequest& request, rpc::v1::RepairPGResponse& response);
void seal_pg(ChunkStore& store, const rpc::v1::SealPGRequest& request, rpc::v1::PGInventoryResponse& response);

void push_replica(ChunkStore& store, const rpc::v1::RepairRequest& request,
                  rpc::v1::RepairResponse& response);
}
