#include <fstream>
#include <iostream>
#include <ctime>

int main(int argc, char* argv[])
{
  if (argc < 2)
  {
    std::cerr << "Invalid argument, array size is needed" << std::endl;
    std::terminate();
  }

  const std::string filename = [argc, argv] {
    if (argc < 3)
    {
      return "int_array";
    }
    return (const char*)argv[2];
  }();

  int len{};
  if (!(len = std::stoi(argv[1]))) {
    std::cerr << "Incorrect size of array" << std::endl;

    std::terminate();
  }

  std::ofstream file{filename, std::ios::binary};
  std::srand(std::time(0));

  for (int i = 0; i < len; ++i)
  {
    for (int j = 0; j < 4; ++j)
    {
      file << (unsigned char)(std::rand() % 256);
    }
  }
  return 0;
}
