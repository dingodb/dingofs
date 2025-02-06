#include <butil/time.h>

#include <cstring>
#include <iostream>

using ::butil::Timer;

int main() {
  Timer timer;

  size_t size = 4 * 1024 * 1024;
  char* ptr = new char[size];

  timer.start();
  memset(ptr, 0, size);
  timer.stop();
  std::cout << "cost " << timer.u_elapsed() * 1.0 / 1e6 << " seconds"
            << std::endl;

  timer.start();
  memset(ptr, 0, size);
  timer.stop();
  std::cout << "cost " << timer.u_elapsed() * 1.0 / 1e6 << " seconds"
            << std::endl;
  return 0;
}
