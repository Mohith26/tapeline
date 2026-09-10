#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace tapeline {

template <class T>
struct raw_integer {
  using type = T;
};
template <class T>
  requires std::is_enum_v<T>
struct raw_integer<T> {
  using type = std::underlying_type_t<T>;
};
template <class T>
using raw_integer_t = typename raw_integer<T>::type;

// 64-bit FNV-1a over little-endian bytes. It is not fast and it is not
// cryptographic; it is simple enough to reimplement in twenty lines of Python
// so the feed decoder in tools/ can prove it rebuilt the same book.
class Fnv1a64 {
 public:
  static constexpr std::uint64_t kOffset = 0xcbf29ce484222325ull;
  static constexpr std::uint64_t kPrime = 0x100000001b3ull;

  void add_bytes(const void* data, std::size_t n) noexcept {
    const auto* p = static_cast<const unsigned char*>(data);
    for (std::size_t i = 0; i < n; ++i) {
      h_ ^= p[i];
      h_ *= kPrime;
    }
  }

  template <class T>
    requires std::is_integral_v<T> || std::is_enum_v<T>
  void add(T v) noexcept {
    using U = std::make_unsigned_t<raw_integer_t<T>>;
    U u = static_cast<U>(v);
    if constexpr (std::endian::native == std::endian::big) u = std::byteswap(u);
    unsigned char buf[sizeof(U)];
    std::memcpy(buf, &u, sizeof(U));
    add_bytes(buf, sizeof(U));
  }

  [[nodiscard]] std::uint64_t value() const noexcept { return h_; }

 private:
  std::uint64_t h_ = kOffset;
};

} // namespace tapeline
