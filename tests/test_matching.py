"""二分图匹配引擎单测：无解、唯一完整对应、多解（两份不同对应）。

直接构造行与候选边，不依赖数据库，专门锁定对账判定口径。
"""
from __future__ import annotations

import pytest

from app.matching import find_two_matchings


def test_no_solution_when_a_line_has_no_candidate():
    # L2 没有任何候选明细
    matchings = find_two_matchings(["L1", "L2"], {"L1": [1, 2], "L2": []})
    assert matchings == []


def test_no_solution_when_two_lines_share_single_record():
    # 两行都只能用同一条明细 -> 无法同时覆盖
    matchings = find_two_matchings(["L1", "L2"], {"L1": [9], "L2": [9]})
    assert matchings == []


def test_unique_matching_returns_single_result():
    # L1 -> r1；L2 -> r2（r1 不满足 L2），唯一完整对应
    matchings = find_two_matchings(
        ["L1", "L2"], {"L1": [1, 2], "L2": [2]}
    )
    assert len(matchings) == 1
    assert matchings[0] == {"L1": 1, "L2": 2}


def test_surplus_records_allowed_and_left_unused():
    # 明细允许剩余：3 条候选、2 行，唯一对应时多余明细不被使用
    matchings = find_two_matchings(
        ["L1", "L2"], {"L1": [1, 2, 3], "L2": [3]}
    )
    assert len(matchings) == 1
    used = set(matchings[0].values())
    assert len(used) == 2 and matchings[0]["L2"] == 3


def test_ambiguous_returns_two_distinct_complete_matchings():
    # 经典 2x2 完全二分图：两份完整对应
    matchings = find_two_matchings(["L1", "L2"], {"L1": [1, 2], "L2": [1, 2]})
    assert len(matchings) == 2
    m1, m2 = matchings
    assert m1 != m2
    for m in (m1, m2):
        assert set(m) == {"L1", "L2"}          # 覆盖全部行
        assert len(set(m.values())) == 2        # 明细不重复
        assert m["L1"] in (1, 2) and m["L2"] in (1, 2)


def test_ambiguous_cycle_embedded_in_larger_graph():
    # 三行：L1 只连 r1，L2/L3 与 r2/r3 构成交替环
    adjacency = {
        "L1": [1],
        "L2": [2, 3],
        "L3": [2, 3],
    }
    matchings = find_two_matchings(["L1", "L2", "L3"], adjacency)
    assert len(matchings) == 2
    m1, m2 = matchings
    # 固定部分不变，环上的两行指派互换
    assert m1["L1"] == m2["L1"] == 1
    assert m1["L2"] == m2["L3"] and m1["L3"] == m2["L2"]
    # 两份都不重复使用明细
    for m in (m1, m2):
        assert len(set(m.values())) == 3


def test_deterministic_output_across_runs():
    adjacency = {"L1": [3, 1, 2], "L2": [2, 1], "L3": [1, 2, 3]}
    first = find_two_matchings(["L1", "L2", "L3"], adjacency)
    for _ in range(20):
        again = find_two_matchings(["L1", "L2", "L3"], adjacency)
        assert again == first


def test_string_line_numbers_supported():
    matchings = find_two_matchings(
        ["row-01", "row-02"], {"row-01": [10], "row-02": [10, 11]}
    )
    assert len(matchings) == 1
    assert matchings[0] == {"row-01": 10, "row-02": 11}


@pytest.mark.parametrize("n", [2, 5, 40, 80])
def test_permutation_graph_has_multiple_matchings(n):
    # n 行 n 条完全二分图（n>=2）：至少两份对应，且两份都完整可用
    records = list(range(100, 100 + n))
    adjacency = {f"L{i}": list(records) for i in range(n)}
    matchings = find_two_matchings(list(adjacency), adjacency)
    assert len(matchings) == 2
    for m in matchings:
        assert len(m) == n and len(set(m.values())) == n


def test_single_line_single_record_is_unique():
    matchings = find_two_matchings(["L0"], {"L0": [100]})
    assert len(matchings) == 1 and matchings[0] == {"L0": 100}
