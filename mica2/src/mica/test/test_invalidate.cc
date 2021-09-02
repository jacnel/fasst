#include "mica/table/ctable.h"
#include "mica/util/hash.h"
#include "mica/util/lcore.h"

#include <iostream>

struct CTableConfig : public ::mica::table::BasicLossyCTableConfig {
  static constexpr bool kCollectStats = true;
};

typedef ::mica::table::CTable<CTableConfig> Table;
typedef ::mica::table::Result Result;

template <typename T>
static uint64_t hash(const T* key, size_t key_length) {
  return ::mica::util::hash(key, key_length);
}

int main() {
  ::mica::util::lcore.pin_thread(0);

  auto config = ::mica::util::Config::load_file("test_invalidate.json");

  CTableConfig::Alloc alloc(config.get("alloc"));
  CTableConfig::Pool pool(config.get("pool"), &alloc);

  std::vector<size_t> evicted_keys;
  CTableConfig::EvictionCallback callback =
      [&evicted_keys](const char* key, size_t key_len, const char* value,
                      size_t value_len) {
        evicted_keys.push_back(*(reinterpret_cast<const size_t*>(key)));
      };
  Table table(config.get("table"), &alloc, &pool, callback);

  size_t num_items = 1024;

  size_t key_i;
  size_t value_i;
  const char* key = reinterpret_cast<const char*>(&key_i);
  char* value = reinterpret_cast<char*>(&value_i);

  bool first_failure = false;
  size_t first_failure_i = 0;
  size_t last_failure_i = 0;
  size_t success_count = 0;

  for (size_t i = 0; i < num_items; i++) {
    key_i = i;
    value_i = -i;
    uint64_t key_hash = hash(&key_i, sizeof(key_i));
    uint64_t curr_version;
    Result res;

    res = table.prepare(key_hash, key, sizeof(key_i), sizeof(value_i),
                        &curr_version);
    assert(res == Result::kSuccess);
    res = table.finalize(key_hash, key, sizeof(key_i), value, sizeof(value_i),
                         curr_version, /*deleted=*/false);
    assert(res == Result::kSuccess);

    // Immediately set again to make the item modified.
    value_i = i + 1;
    key_hash = hash(&key_i, sizeof(key_i));
    if (i % 2 == 0) {
      res = table.update(key_hash, key, sizeof(key_i), value, sizeof(value_i));
      assert(res == Result::kSuccess);
    } else {
      res = table.del(key_hash, key, sizeof(key_i));
      assert(res == Result::kSuccess);
    }
  }

  for (size_t i = 0; i < num_items; i++) {
    key_i = i;
    size_t value_len;
    uint64_t version;
    uint64_t key_hash = hash(&key_i, sizeof(key_i));

    if (table.get(key_hash, key, sizeof(key_i), value, sizeof(value_i),
                  &value_len, &version, false) == Result::kSuccess)
      success_count++;
    else {
      if (!first_failure) {
        first_failure = true;
        first_failure_i = i;
      }
      last_failure_i = i;
    }
  }

  for (size_t i = 0; i < num_items; i++) {
    key_i = i;
    uint64_t key_hash = hash(&key_i, sizeof(key_i));
    table.invalidate(key_hash);
  }

  printf("first_failure: %zu (%.2f%%)\n", first_failure_i,
         100. * (double)first_failure_i / (double)num_items);
  printf("last_failure: %zu (%.2f%%)\n", last_failure_i,
         100. * (double)last_failure_i / (double)num_items);
  printf("success_count: %zu (%.2f%%)\n", success_count,
         100. * (double)success_count / (double)num_items);

  table.print_stats();

  printf("Evicted keys:\n");
  int newline = 1;
  for (auto& k : evicted_keys) {
    printf("%-10lu", k);
    if (newline % 10 == 0) {
      printf("\n");
    }
    ++newline;
  }
  printf("\n");

  return EXIT_SUCCESS;
}
