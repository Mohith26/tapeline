CXX = clang++
CXXFLAGS = -std=c++23 -O2 -Wall -Wextra -Wpedantic -Iinclude
LDFLAGS =
BUILD = build

HEADERS = $(wildcard include/tapeline/*.hpp)
TEST_SRCS = $(wildcard tests/*.cpp)
BINS = $(BUILD)/exchange $(BUILD)/client $(BUILD)/feed $(BUILD)/bench $(BUILD)/replayer

.PHONY: all test test-asan bench e2e clean

all: $(BINS) $(BUILD)/tests

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/tests: $(TEST_SRCS) tests/test.hpp $(HEADERS) | $(BUILD)
	$(CXX) $(CXXFLAGS) -Itests $(TEST_SRCS) -o $@ $(LDFLAGS)

$(BUILD)/tests-asan: $(TEST_SRCS) tests/test.hpp $(HEADERS) | $(BUILD)
	$(CXX) $(CXXFLAGS) -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -Itests $(TEST_SRCS) -o $@ $(LDFLAGS)

$(BUILD)/%: src/%_main.cpp $(HEADERS) | $(BUILD)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

test: $(BUILD)/tests
	./$(BUILD)/tests "" results/tests.json

test-asan: $(BUILD)/tests-asan
	./$(BUILD)/tests-asan "" results/tests_asan.json

bench: $(BUILD)/bench
	./$(BUILD)/bench results/bench.json

e2e: $(BUILD)/exchange $(BUILD)/client $(BUILD)/feed
	./scripts/e2e.sh

clean:
	rm -rf $(BUILD)
