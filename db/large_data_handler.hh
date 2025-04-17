/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include "schema/schema_fwd.hh"
#include "system_keyspace.hh"
#include "sstables/shared_sstable.hh"
#include "utils/updateable_value.hh"
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics.hh>
#include "cql3/untyped_result_set.hh"
#include "sstables/sstables.hh"

namespace sstables {
class sstable;
class key;
}

namespace db {

template <typename T> static std::string key_to_str(const T& key, const schema& s) {
    return fmt::to_string(key.with_schema(s));
}

class system_keyspace;

class large_data_handler {
public:
    struct stats {
        int64_t partitions_bigger_than_threshold = 0; // number of large partition updates exceeding threshold_bytes
        int64_t rows_bigger_than_threshold = 0; // number of large row updates exceeding threshold_bytes
    };

    // In order to optimize memory usage and prevent excessive memory consumption,
    // here only retore neccessary information
    // table_name → (pk_ck, row_size)
    mutable std::unordered_map<std::string, std::unordered_map<std::string, int64_t>> large_rows;

    void add_large_row_metrics(std::string table_name, std::string pk_ck) const {
        namespace sm = seastar::metrics;
        static sm::label column_family_label("table");

        _metrics.add_group("database", {
            sm::make_gauge("large_rows_info", large_rows[table_name][pk_ck],
            sm::description("Row size of the newest large row in a table."),
            {column_family_label(table_name)})
            .aggregate({sm::shard_label})
            .set_skip_when_empty(),
        });
    }

private:
    // Assuming:
    // * there is at most one log entry every 1MB
    // * the average latency of the log is 4ms (depends on the load)
    // * we aim to sustain 1GB/s of write bandwidth
    // We need a concurrency of:
    //  C = (1GB/s / 1MB) * 4ms = 1k/s * 4ms = 4
    // 16 should be enough for everybody.
    static constexpr size_t max_concurrency = 16;
    semaphore _sem{max_concurrency};

    // A convenience function for using the above semaphore. Unlike the global with_semaphore, this will not wait on the
    // future returned by func. The objective is for the future returned by func to run in parallel with whatever the
    // caller is doing, but limit how far behind we can get.
    template<typename Func>
    future<> with_sem(Func&& func) {
        return get_units(_sem, 1).then([func = std::forward<Func>(func)] (auto units) mutable {
            // Future is discarded purposefully, see method description.
            // FIXME: error handling.
            (void)func().finally([units = std::move(units)] {});
        });
    }

    bool _running = false;

protected:
    uint64_t _partition_threshold_bytes;
    uint64_t _row_threshold_bytes;
    uint64_t _cell_threshold_bytes;
    uint64_t _rows_count_threshold;
    uint64_t _collection_elements_count_threshold;
    mutable large_data_handler::stats _stats;

protected:
    seastar::shared_ptr<db::system_keyspace> _sys_ks;
    mutable seastar::metrics::metric_groups _metrics;

public:
    explicit large_data_handler(uint64_t partition_threshold_bytes, uint64_t row_threshold_bytes, uint64_t cell_threshold_bytes, uint64_t rows_count_threshold, uint64_t collection_elements_count_threshold);
    virtual ~large_data_handler() {}

    // Once large_data_handler is stopped no further updates will be accepted.
    bool running() const { return _running; }
    void start();
    future<> stop();

    future<bool> reset_large_row_size(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size) {
        assert(running());
        const schema &s = *sst.get_schema();

        std::string pk_str = key_to_str(partition_key.to_partition_key(s), s);
        // 如果是 clustering key 则将其转换为 string; 如果是 static row, 则应该为空
        std::string ck_str = clustering_key ? key_to_str(*clustering_key, s) : "";
        auto pk_ck = pk_str + ", " + ck_str;

        if (large_rows[s.cf_name()].contains(pk_ck)) {
            const std::string query = format("SELECT * FROM system.large_rows WHERE keyspace_name = ? AND table_name = ? AND partition_key = ? AND clustering_key = ? ALLOW FILTERING");
            auto result = _sys_ks->execute_cql(query, s.ks_name(), s.cf_name(), pk_str, ck_str).get();

            // 如果结果为空，说明该 large row 已经被更新过，不存在了；否则保留原来的值
            if (result->empty()) {
                large_rows[s.cf_name()][pk_ck] = 0;
            }
        }
        return make_ready_future<bool>(false);
    }

    future<bool> maybe_record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size) {
        assert(running());
        if (__builtin_expect(row_size > _row_threshold_bytes, false)) {
            return with_sem([&sst, &partition_key, clustering_key, row_size, this] {
                return record_large_rows(sst, partition_key, clustering_key, row_size);
            }).then([] {
                return true;
            });
        } else { // 重置不存在的 large row size
            return reset_large_row_size(sst, partition_key, clustering_key, row_size);
        }
        return make_ready_future<bool>(false);
    }

    struct partition_above_threshold {
        bool size = false;
        bool rows = false;
    };
    future<partition_above_threshold> maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows);

    future<bool> maybe_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) {
        assert(running());
        if (__builtin_expect(cell_size > _cell_threshold_bytes || collection_elements > _collection_elements_count_threshold, false)) {
            return with_sem([&sst, &partition_key, clustering_key, &cdef, cell_size, collection_elements, this] {
                return record_large_cells(sst, partition_key, clustering_key, cdef, cell_size, collection_elements);
            }).then([] {
                return true;
            });
        }
        return make_ready_future<bool>(false);
    }

    future<> maybe_delete_large_data_entries(sstables::shared_sstable sst);

    const large_data_handler::stats& stats() const { return _stats; }

    uint64_t get_partition_threshold_bytes() const noexcept {
        return _partition_threshold_bytes;
    }
    uint64_t get_row_threshold_bytes() const noexcept {
        return _row_threshold_bytes;
    }
    uint64_t get_cell_threshold_bytes() const noexcept {
        return _cell_threshold_bytes;
    }
    uint64_t get_rows_count_threshold() const noexcept {
        return _rows_count_threshold;
    }
    uint64_t get_collection_elements_count_threshold() const noexcept {
        return _collection_elements_count_threshold;
    }

    static sstring sst_filename(const sstables::sstable& sst);

    void plug_system_keyspace(db::system_keyspace& sys_ks) noexcept;
    void unplug_system_keyspace() noexcept;

protected:
    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const = 0;
    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key, const clustering_key_prefix* clustering_key, uint64_t row_size) const = 0;
    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const = 0;
    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows) const = 0;
};

class cql_table_large_data_handler : public large_data_handler {
    gms::feature_service& _feat;
    std::function<future<> (const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements)> _record_large_cells;
    std::optional<std::any> _feat_listener;

    static constexpr uint64_t MB = 1024 * 1024;

    using threshold_updater = utils::transforming_value_updater<uint64_t, uint32_t>;
    threshold_updater _partition_threshold_mb_updater;
    threshold_updater _row_threshold_mb_updater;
    threshold_updater _cell_threshold_mb_updater;
    threshold_updater _rows_count_threshold_updater;
    threshold_updater _collection_elements_count_threshold_updater;
public:
    explicit cql_table_large_data_handler(gms::feature_service& feat,
            utils::updateable_value<uint32_t> partition_threshold_mb,
            utils::updateable_value<uint32_t> row_threshold_mb,
            utils::updateable_value<uint32_t> cell_threshold_mb,
            utils::updateable_value<uint32_t> rows_count_threshold,
            utils::updateable_value<uint32_t> collection_elements_count_threshold);

protected:
    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows) const override;
    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const override;
    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const override;
    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key, const clustering_key_prefix* clustering_key, uint64_t row_size) const override;
    future<> fetch_all_large_rows(const std::string& keyspace_name, const std::string& table_name, const std::string& partition_key, const std::string& clustering_key = "") const;

private:
    future<> internal_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const;
    future<> internal_record_large_cells_and_collections(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const;

private:
    template <typename... Args>
    future<> try_record(std::string_view large_table, const sstables::sstable& sst,  const sstables::key& partition_key, int64_t size,
            std::string_view desc, std::string_view extra_path, const std::vector<sstring> &extra_fields, Args&&... args) const;
};

class nop_large_data_handler : public large_data_handler {
public:
    nop_large_data_handler();
    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows) const override {
        return make_ready_future<>();
    }

    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const override {
        return make_ready_future<>();
    }

    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const override {
        return make_ready_future<>();
    }

    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size) const override {
        return make_ready_future<>();
    }
};

}
