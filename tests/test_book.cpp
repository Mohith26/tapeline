#include "tapeline/book.hpp"
#include "test.hpp"

#include <string>
#include <vector>

using namespace tapeline;

namespace {

struct Fill {
  OrderId id;
  Qty qty;
  Price price;
  Qty leaves;
};

std::vector<Fill> run(Book& b, Side side, Price limit, Qty qty, Qty* leaves_out = nullptr) {
  std::vector<Fill> fills;
  const Qty leaves = b.match(side, limit, qty, [&](const Book::Order& o, Qty q, Price px) {
    fills.push_back({o.id, q, px, o.qty});
  });
  if (leaves_out) *leaves_out = leaves;
  return fills;
}

} // namespace

TEST(book_rest_and_best) {
  Book b(0);
  CHECK(!b.best_bid());
  CHECK(!b.best_ask());
  b.rest(1, Side::Buy, 100, 10, 1, 1);
  b.rest(2, Side::Buy, 101, 5, 1, 2);
  b.rest(3, Side::Sell, 105, 7, 1, 3);
  b.rest(4, Side::Sell, 103, 2, 1, 4);
  CHECK_EQ(*b.best_bid(), 101);
  CHECK_EQ(*b.best_ask(), 103);
  CHECK_EQ(b.live_orders(), std::size_t{4});
  CHECK_EQ(b.levels(Side::Buy), std::size_t{2});
  CHECK_EQ(b.levels(Side::Sell), std::size_t{2});
  std::string why;
  CHECK_MSG(b.check_invariants(&why), why);
}

TEST(book_price_then_time_priority) {
  Book b(0);
  b.rest(1, Side::Sell, 101, 5, 1, 1);
  b.rest(2, Side::Sell, 100, 5, 1, 2); // better price, arrives later
  b.rest(3, Side::Sell, 100, 5, 1, 3); // same price, behind 2
  Qty leaves = 0;
  auto fills = run(b, Side::Buy, 101, 12, &leaves);
  CHECK_EQ(leaves, 0u);
  CHECK_EQ(fills.size(), std::size_t{3});
  CHECK_EQ(fills[0].id, 2u);
  CHECK_EQ(fills[0].price, 100);
  CHECK_EQ(fills[1].id, 3u);
  CHECK_EQ(fills[2].id, 1u);
  CHECK_EQ(fills[2].qty, 2u);
  CHECK_EQ(fills[2].leaves, 3u);
  CHECK_EQ(b.live_orders(), std::size_t{1});
  CHECK_EQ(*b.best_ask(), 101);
  std::string why;
  CHECK_MSG(b.check_invariants(&why), why);
}

TEST(book_limit_respects_price) {
  Book b(0);
  b.rest(1, Side::Sell, 100, 5, 1, 1);
  b.rest(2, Side::Sell, 102, 5, 1, 2);
  Qty leaves = 0;
  auto fills = run(b, Side::Buy, 101, 8, &leaves);
  CHECK_EQ(fills.size(), std::size_t{1});
  CHECK_EQ(fills[0].qty, 5u);
  CHECK_EQ(leaves, 3u);
  CHECK_EQ(b.live_orders(), std::size_t{1});
  CHECK_EQ(*b.best_ask(), 102);
}

TEST(book_market_sweeps_levels) {
  Book b(0);
  for (OrderId i = 1; i <= 5; ++i) b.rest(i, Side::Buy, 100 - static_cast<Price>(i), 10, 1, i);
  Qty leaves = 0;
  auto fills = run(b, Side::Sell, kMinPrice, 45, &leaves);
  CHECK_EQ(fills.size(), std::size_t{5});
  CHECK_EQ(leaves, 0u);
  CHECK_EQ(fills[0].price, 99);
  CHECK_EQ(fills[4].price, 95);
  CHECK_EQ(fills[4].qty, 5u);
  CHECK_EQ(b.live_orders(), std::size_t{1});
  CHECK_EQ(*b.best_bid(), 95);
  CHECK_EQ(b.best_level(Side::Buy)->total, 5u);
}

TEST(book_cancel_and_reduce) {
  Book b(0);
  auto s1 = b.rest(1, Side::Buy, 100, 10, 1, 1);
  auto s2 = b.rest(2, Side::Buy, 100, 20, 1, 2);
  auto s3 = b.rest(3, Side::Buy, 100, 30, 1, 3);
  CHECK_EQ(b.best_level(Side::Buy)->total, 60u);
  CHECK_EQ(b.reduce(s2, 5), 15u);
  CHECK_EQ(b.best_level(Side::Buy)->total, 55u);
  auto removed = b.remove(s1);
  CHECK_EQ(removed.id, 1u);
  CHECK_EQ(b.best_level(Side::Buy)->count, 2u);
  auto fills = run(b, Side::Sell, 100, 100);
  CHECK_EQ(fills.size(), std::size_t{2});
  CHECK_EQ(fills[0].id, 2u); // 2 kept its place ahead of 3 after shrinking
  CHECK_EQ(fills[0].qty, 15u);
  CHECK_EQ(fills[1].id, 3u);
  (void)s3;
  CHECK_EQ(b.live_orders(), std::size_t{0});
  CHECK(!b.best_bid());
  std::string why;
  CHECK_MSG(b.check_invariants(&why), why);
}

TEST(book_remove_middle_of_queue) {
  Book b(0);
  auto a = b.rest(1, Side::Sell, 50, 1, 1, 1);
  auto m = b.rest(2, Side::Sell, 50, 1, 1, 2);
  auto z = b.rest(3, Side::Sell, 50, 1, 1, 3);
  b.remove(m);
  std::string why;
  CHECK_MSG(b.check_invariants(&why), why);
  b.remove(z);
  CHECK_MSG(b.check_invariants(&why), why);
  b.remove(a);
  CHECK_MSG(b.check_invariants(&why), why);
  CHECK_EQ(b.levels(Side::Sell), std::size_t{0});
}

TEST(book_slot_reuse) {
  Book b(0, 4);
  std::vector<std::uint32_t> slots;
  for (OrderId i = 1; i <= 100; ++i) slots.push_back(b.rest(i, Side::Buy, 10, 1, 1, i));
  for (auto s : slots) b.remove(s);
  const auto again = b.rest(200, Side::Buy, 10, 1, 1, 200);
  CHECK(again < 100); // came off the free list, no fresh slab growth
  std::string why;
  CHECK_MSG(b.check_invariants(&why), why);
}

TEST(book_depth_and_hash) {
  Book b(0);
  b.rest(1, Side::Buy, 100, 10, 1, 1);
  b.rest(2, Side::Buy, 100, 5, 1, 2);
  b.rest(3, Side::Buy, 99, 7, 1, 3);
  b.rest(4, Side::Sell, 101, 1, 1, 4);
  auto bids = b.depth(Side::Buy, 5);
  CHECK_EQ(bids.size(), std::size_t{2});
  CHECK_EQ(bids[0].first, 100);
  CHECK_EQ(bids[0].second, 15u);
  CHECK_EQ(bids[1].first, 99);
  const auto h1 = b.hash();
  Book c(0);
  c.rest(3, Side::Buy, 99, 7, 9, 9);
  c.rest(4, Side::Sell, 101, 1, 9, 9);
  c.rest(1, Side::Buy, 100, 10, 9, 9);
  c.rest(2, Side::Buy, 100, 5, 9, 9);
  CHECK_EQ(h1, c.hash()); // session and client ids are not part of the state
  Book d(0);
  d.rest(2, Side::Buy, 100, 5, 9, 9);
  d.rest(1, Side::Buy, 100, 10, 9, 9);
  d.rest(3, Side::Buy, 99, 7, 9, 9);
  d.rest(4, Side::Sell, 101, 1, 9, 9);
  CHECK(h1 != d.hash()); // queue order at a level is part of the state
}
