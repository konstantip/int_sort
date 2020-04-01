#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <list>
#include <fstream>
#include <thread>
#include <experimental/filesystem>
#include <stdexcept>

#include <notifying_queue.hpp>

template <typename T>
T deserializeForStoringQueue(std::string&&);

template <>
std::string deserializeForStoringQueue<std::string>(std::string&& str)
{
  return std::move(str);
}

template <typename T>
std::string serializeForStoringQueue(T&&);

template<>
std::string serializeForStoringQueue<std::string>(std::string&& str)
{
  return std::move(str);
}

class Memory final
{
 public:
  using ElType = uint32_t;

  explicit Memory(const std::size_t threads_count, const std::size_t num_of_byte) :
      memory_per_thread_{(num_of_byte / threads_count) / sizeof(ElType)},
      threads_count_{threads_count},
      threads_memory_meta_{new ThreadMemory[threads_count]{}},
      function_{new std::size_t[threads_count_]},
      memory_{new ElType[memory_per_thread_ * threads_count]}
  {
    bool used_blocks[threads_count];
    for (std::size_t i{}; i < threads_count; used_blocks[i++] = false)
      ;

    {
      auto& el = threads_memory_meta_[0];
      el.begin = memory_.get();
      el.available_size = memory_per_thread_;
      el.thread_running = true;
      function_[0] = 0;
    }
    used_blocks[0] = true;
    for (std::size_t thread_num = 1; thread_num < threads_count;)
    {
      std::size_t prev_index = 0;
      for (std::size_t i = 1; i < threads_count; ++i)
      {
        if (used_blocks[i])
        {
          if (i - prev_index == 1)
          {
            prev_index = i;
            continue;
          }

          {
            const std::size_t index_of_data = prev_index + (i - prev_index) / 2;
	    function_[index_of_data] = thread_num;

            auto& el = threads_memory_meta_[thread_num];

            el.begin = &memory_[index_of_data * memory_per_thread_];
            el.available_size = memory_per_thread_;
            el.thread_running = true;

            used_blocks[index_of_data] = true;
            ++thread_num;
            prev_index = i;
          }
        }
      }

      {
        const std::size_t index_of_data = prev_index + (threads_count_ - prev_index) / 2;
	function_[index_of_data] = thread_num;
        auto& el = threads_memory_meta_[thread_num];
        el.begin = &memory_[index_of_data * memory_per_thread_];
        el.available_size = memory_per_thread_;
        el.thread_running = true;

        used_blocks[index_of_data] = true;
        ++thread_num;
      }
    }
  }

  [[nodiscard]] ElType* memoryBegin() const noexcept
  {
    return threads_memory_meta_[memory_index_].begin;
  }

  void finishThread() const noexcept
  {
    threads_memory_meta_[memory_index_].thread_running.exchange(false, std::memory_order_relaxed);
  }

  void tryBorrow() const noexcept
  {
    const auto calculate_next_index = [
	start_buffer_ptr{memory_.get()},
        &available_mem{threads_memory_meta_[memory_index_].available_size},
	begin{threads_memory_meta_[memory_index_].begin},
        memory_per_thread{memory_per_thread_}]() noexcept -> std::size_t {
      return (begin - start_buffer_ptr + available_mem) / memory_per_thread;
    };

    const auto index_exists = [size{threads_count_}](const std::size_t index) {
      return index < size;
    };

    for (auto next_index = calculate_next_index(); index_exists(next_index); next_index = calculate_next_index())
    {
      next_index = function_[next_index];
      if (threads_memory_meta_[next_index].thread_running.load())
      {
        break;
      }

      threads_memory_meta_[memory_index_].available_size +=
          threads_memory_meta_[next_index].available_size;
    }
  }

  [[nodiscard]] std::size_t getAvailableSize() const noexcept
  {
    tryBorrow();
    return threads_memory_meta_[memory_index_].available_size;
  }

  void setThreadLocalMemoryIndex(const std::size_t index) noexcept
  {
    memory_index_ = index;
  }
 private:
  struct ThreadMemory
  {
    ElType* begin{};
    std::atomic_size_t available_size{};
    std::atomic_bool thread_running{};
  };

 public:
  const std::size_t memory_per_thread_;
  const std::size_t threads_count_;

  const std::unique_ptr<ThreadMemory[], std::default_delete<ThreadMemory[]>> threads_memory_meta_;
  const std::unique_ptr<std::size_t[], std::default_delete<std::size_t[]>> function_;
  const std::unique_ptr<ElType[], std::default_delete<ElType[]>> memory_;
  inline static thread_local std::size_t memory_index_{0};
};

void finalize(std::ifstream& file, Memory::ElType* begin, const Memory::ElType* const end, std::ofstream& target)
{
  target.write(reinterpret_cast<const char*>(begin), sizeof(*begin) * (end - begin));

  for (; !file.eof();)
  {
    Memory::ElType tmp[10000];

    file.read(reinterpret_cast<char*>(&tmp), sizeof(tmp));
    std::size_t num_of_red_bytes = file.gcount();
    target.write(reinterpret_cast<char*>(&tmp), num_of_red_bytes);
  }
}

void merge(std::ifstream& file1, std::ifstream& file2, std::ofstream& target,
           Memory& memory)
{
  const auto begin_block = memory.memoryBegin();
  Memory::ElType *cnt1, *end1;
  Memory::ElType *beg2, *cnt2, *end2;
  Memory::ElType *merge_beg, * merge_cnt{};
  for (; !file1.eof() || !file2.eof();)
  {
    const std::size_t available_memory = memory.getAvailableSize();

    if (merge_cnt)
    {
      //we need to replace memory

      const auto new_beg2 = begin_block + available_memory / 4;

      if (new_beg2 < end2 && new_beg2 >= cnt2)
      {
        std::move_backward(cnt2, end2, new_beg2 + (end2 - cnt2));
      }
      else
      {
        std::move(cnt2, end2, new_beg2);
      }
      if (begin_block != cnt1)
      {
        std::move(cnt1, end1, begin_block);
      }

      merge_beg = merge_cnt = begin_block + available_memory / 2;
      end2 = new_beg2 + (end2 - cnt2);
      beg2 = cnt2 = new_beg2;

      end1 = begin_block + (end1 - cnt1);
      cnt1 = begin_block;
    }
    else
    {
      cnt1 = end1 = begin_block;
      beg2 = end2 = cnt2 = begin_block + available_memory / 4;
      merge_beg = merge_cnt = begin_block + available_memory / 2;
    }

    {
      
      file1.read(reinterpret_cast<char*>(end1), sizeof(*beg2) * (beg2 - end1));

      end1 += file1.gcount() / sizeof(*end1);
    }

    {
      file2.read(reinterpret_cast<char*>(end2), sizeof(*end2) * (merge_beg - end2));

      end2 += file2.gcount() / sizeof(*end2);
    }

    if (file1.eof() && begin_block == end1)
    {
      finalize(file2, cnt2, end2, target);
      return;
    }
    if (file2.eof() && beg2 == end2)
    {
      finalize(file1, cnt1, end1, target);
      return;
    }

    for (; cnt1 != end1 && cnt2 != end2;)
    {
      auto& least = *cnt1 < *cnt2 ? cnt1 : cnt2;

      *(merge_cnt++) = *(least++);
    }

    target.write(reinterpret_cast<char*>(merge_beg), (merge_cnt - merge_beg) * sizeof(*merge_cnt));
  }
  finalize(file2, cnt2, end2, target);
  finalize(file1, cnt1, end1, target);
}

template <bool is_main_thread>
void processReduce(notifying_queue::DoublePopQueue<std::string>& files_queue,
                   std::atomic_size_t& working_threads_num,
                   std::atomic_size_t& files_enumerator,
                   std::atomic_size_t& num_of_remaining_files,
                   Memory& memory,
                   const std::size_t thread_num)
{
  for (;;)
  {
    std::string filename1;
    std::string filename2;

    const std::size_t remaining_files = num_of_remaining_files.fetch_add(0, std::memory_order_relaxed);

    if (remaining_files == 1)
    {
      files_queue.finish();
    }

    if constexpr (!is_main_thread)
    {
      if (remaining_files / 2 - 1 < thread_num)
      {
        working_threads_num.fetch_sub(1, std::memory_order_relaxed);
        break;
      }
    }

    if constexpr (is_main_thread)
    {
      if (!files_queue.waitAndPop(filename1, filename2))
      {
        files_queue.waitAndPopForce(filename1);

        std::experimental::filesystem::rename(filename1, "result");
        return;
      }
    }
    else
    {
      if (!files_queue.waitAndPop(filename1, filename2))
      {
        working_threads_num.fetch_sub(1, std::memory_order_relaxed);
        return;
      }
    }

    num_of_remaining_files.fetch_sub(1, std::memory_order_relaxed);

    std::string new_file_name{"tmp" + std::to_string(files_enumerator.fetch_add(1, std::memory_order_relaxed))};
    {
      std::ofstream target_file{new_file_name};
      std::ifstream file1{filename1}, file2{filename2};

      merge(file1, file2, target_file, memory);
    }

    files_queue.pushForce(std::move(new_file_name));

    std::experimental::filesystem::remove(filename1);
    std::experimental::filesystem::remove(filename2);
  }
}

template <bool is_main_thread>
struct ThreadAction
{
  void operator()()
  {
    std::atomic_thread_fence(std::memory_order_acquire);
    memory.setThreadLocalMemoryIndex(order_num);

    for (;;)
    {
      const std::size_t available_size = memory.getAvailableSize();
      const std::size_t pos = red_bytes.fetch_add(available_size * sizeof(Memory::ElType), std::memory_order_relaxed);
      if (pos >= file_size)
      {
        red_bytes.fetch_sub(available_size * sizeof(Memory::ElType), std::memory_order_relaxed);
        break;
      }

      num_of_remaining_files.fetch_add(1, std::memory_order_relaxed);

      ifstream.seekg(pos, std::ios::beg);

      const auto block_begin = memory.memoryBegin();

      ifstream.read(reinterpret_cast<char*>(block_begin), sizeof(block_begin[0]) * available_size);
      const std::size_t ints_red = ifstream.gcount() / sizeof(block_begin[0]);

      {
        const std::size_t file_index = files_enumerator.fetch_add(1, std::memory_order_relaxed);

        std::string filename{"tmp" + std::to_string(file_index)};

        {
          std::ofstream file{filename, std::ios::binary};

          std::sort(block_begin, block_begin + ints_red, std::less<Memory::ElType>{});

          file.write(reinterpret_cast<char*>(block_begin), sizeof(block_begin[0]) * ints_red);
          std::cout << std::dec;
        }
        file_names.pushForce(std::move(filename));
      }
    }

    processReduce<is_main_thread>(file_names, num_of_working_threads, files_enumerator,
        num_of_remaining_files, memory, order_num);

    memory.finishThread();
  }
  std::ifstream ifstream;
  const uint64_t file_size;
  std::atomic_uint64_t& red_bytes;

  Memory& memory;

  std::atomic_size_t& num_of_working_threads;
  std::atomic_size_t& num_of_remaining_files;
  notifying_queue::DoublePopQueue<std::string>& file_names;
  std::atomic_size_t& files_enumerator;

  const std::size_t order_num;
};

int main(const int argc, const char* const argv[])
{
  if (argc < 2)
  {
    std::cerr << "File name is needed" << std::endl;
    return -1;
  }

  std::ifstream file{argv[1], std::ifstream::ate | std::ifstream::binary};
  if (file.fail())
  {
    std::cerr << "Fail to open file " << argv[0] <<  "0 item" << std::endl;
    return -2;
  }

  const std::size_t num_of_threads = []() noexcept -> std::size_t {
    const unsigned hw_c = std::thread::hardware_concurrency();
    if (hw_c)
    {
      return hw_c;
    }
    return 4;
  }();

  //2 Mb to queue and 1 for metainfo in constructors
  constexpr uint32_t available_memory = (128 - 3) * 1024 * 1024;

  Memory memory{num_of_threads, available_memory};

  const std::size_t num_of_threads_to_create{num_of_threads - 1};

  std::atomic_size_t num_of_working_threads{1};
  std::atomic_size_t num_of_remaining_files{0};
  std::atomic_size_t files_enumerator{0};

  std::atomic_uint64_t red_bytes{0};

  notifying_queue::DoublePopQueue<std::string> file_names;


  uint64_t file_size = file.tellg();
  file.close();

  std::atomic_thread_fence(std::memory_order_release);

  for (std::size_t i{}; i < num_of_threads_to_create; ++i)
  {
    file.open(argv[1], std::ios::binary);
    if (file.fail())
    {
      std::cerr << "Can not open file, " << i << " item" << std::endl;
      std::terminate();
    }
    ThreadAction<false> action{std::move(file), file_size, red_bytes, memory, num_of_working_threads, num_of_remaining_files,
                        file_names,
                        files_enumerator, i + 1};

    num_of_working_threads.fetch_add(1, std::memory_order_relaxed);
    std::thread t{std::move(action)};
    t.detach();
  }

  file.open(argv[1], std::ios::binary);
  if (file.fail())
  {
    std::cerr << "Can not open file for main thread" << std::endl;
    std::terminate();
  }

  ThreadAction<true>{std::move(file), file_size, red_bytes, memory,
               num_of_working_threads, num_of_remaining_files,
               file_names,
               files_enumerator, 0}();

  return 0;
}
