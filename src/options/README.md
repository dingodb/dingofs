DingoFS options module
===

Basic Usage
---

main.toml:

```toml
title = "person description"

[person]
name = "Jack"
age = 18
```

main.cpp:

```cpp
#include "dingofs/options/options.h"

class PersonOption : public BaseOption {
    BIND_string(name, "none", "person name");
    BIND_int32(age, 0, "person age");
};

class GlobalOption : public BaseOption {
    BIND_string(title, "none", "title");
    BIND_suboption(person_option, "person", PersonOption);
};

int main() {
    GlobalOption option;
    if (!option.Parse("main.toml")) {
        return -1;
    }

    std::cout << "title: " << option.title() << std::endl;
    std::cout << "person.name: " << option.person().name() << std::endl;
    std::cout << "person.age: " << option.person().age() << std::endl;

    return 0;
}
```

Support Onfly
---

main.toml:

```toml
rpc_timeout_s = 3
```

main.cpp:

```cpp
#include "dingofs/options/options.h"

DEFINE_ONFLY_int32(dingofs_meta_rpc_timeout_s, 1, "meta rpc timeout (seconds)")

class GlobalOption : public BaseOption {
    BIND_ONFLY_int32(rpc_timeout_s, dingofs_meta_rpc_timeout_s);
};

int main() {
    GlobalOption option;
    std::cout << "rpc_timeout_s: " << option.rpc_timeout_s() << std::endl;  // 1

    if (!option.Parse("main.toml")) {
        return -1;
    }
    std::cout << "rpc_timeout_s: " << option.rpc_timeout_s() << std::endl;  // 3

    FLAGS_dingofs_meta_rpc_timeout_s = 10;
    std::cout << "rpc_timeout_s: " << option.rpc_timeout_s() << std::endl;  // 10

    return 0;
}
```
