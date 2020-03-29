#include <cstdint>
#include <fstream>
#include <iostream>
#include <unordered_set>

int main(const int argc, const char* const argv[])
{
  if (argc < 3)
  {
    std::cerr << "File names is needed!!!" << std::endl;
    return -1;
  }

  std::ifstream result{argv[1], std::ios::binary};

  if (result.fail())
  {
    std::cerr << "Fail to open file " << argv[1] << std::endl;
    return -2;
  }

  std::ifstream source{argv[2], std::ios::binary};
  if (source.fail())
  {
    std::cerr << "Fail to open file " << argv[1] << std::endl;
    return -2;
  }


  std::unordered_multiset<uint32_t> map;

  {
    uint32_t prev;
    result >> prev;
    if (!result.eof())
    {
      for (;;)
      {
        map.emplace(prev);
        uint32_t current;
        result >> current;
        if (result.eof())
        {
          break;
        }
        if (current < prev)
        {
          return 1;
        }
        prev = current;
      }
    }
  }


  {
    std::unordered_multiset<uint32_t> missed;

    for (;;)
    {
      uint32_t current;
      source >> current;
      if (source.eof())
      {
        break;
      }

      const auto it = map.find(current);

      if (it == map.cend())
      {
        missed.insert(std::move(current));
	continue;
      }

      map.erase(it);
    }

    if (map.empty() && missed.empty())
    {
      return 0;
    }

    std::ofstream fails{"diff", std::ios::binary};
    
    if (!missed.empty())
    {
      fails << "Missed strings: \n";

      for (const auto& el : missed)
      {
        fails << el;
      }
    }
    if (!map.empty())
    {
      fails << "additional strings: \n";

      for (const auto& el : map)
      {
        fails << el;
      }
    }

    return 2;
  }


  return 0;
}
