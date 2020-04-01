cmake_minimum_required(VERSION 3.12)
project(sort)

set(CMAKE_CXX_STANDARD 20)
#set(CMAKE_CXX_STANDARD 17)

add_compile_options(-O3 -W -Wall)
#add_compile_options(-O0 -g -fsanitize=address -fno-omit-frame-pointer -W -Wall)


add_executable(sort main.cpp)

target_link_libraries(sort PRIVATE pthread stdc++fs)
#target_link_libraries(sort PRIVATE asan)

target_include_directories(sort PRIVATE .)
