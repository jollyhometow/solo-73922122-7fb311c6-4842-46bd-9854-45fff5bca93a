"""清单对账的二分图匹配引擎（纯函数，无数据库/IO 依赖）。

问题建模
--------
- 左顶点 L：船方清单行（唯一行号），每行必须被覆盖一次；
- 右顶点 R：受理范围内已入仓的净重明细，每条明细最多使用一次，允许剩余；
- 边 (line, record)：箱号相同且明细净重在清单行要求的闭区间 [min, max] 内。

判定口径
--------
1. 不存在覆盖全部清单行的匹配（完整对应）-> 无解（REJECTED）；
2. 恰好一份完整对应 -> 通过（PASSED），返回该对应；
3. 存在多份完整对应 -> 多解（AMBIGUOUS），返回两份不同的完整对应。

算法
----
- 第一份完整对应：Kuhn 增广路算法（左顶点 <= 80，递归深度安全）；
- 唯一性判定：构造有向图 D——非匹配边方向 L→R，匹配边方向 R→L。
  M 唯一当且仅当 D 中不存在有向环；环上的边翻转（匹配/非匹配互换）
  即得到另一份完整对应 M2。

参考：二分图完美匹配唯一性的标准结论（交替环 ↔ 多重完美匹配）。
"""
from __future__ import annotations

from typing import Hashable, Sequence


def find_two_matchings(
    lines: Sequence[Hashable],
    adjacency: dict[Hashable, Sequence[Hashable]],
) -> list[dict[Hashable, Hashable]]:
    """返回 0、1 或 2 份完整对应（每份为 {行号: 明细 id}）。

    参数
    ----
    lines:
        清单行标识的有序序列（决定输出的确定性顺序）。
    adjacency:
        每行可选的明细 id 序列（调用方已按箱号 + 闭区间过滤）；
        同一明细可出现在多行的候选里，但一份对应中最多被使用一次。
    """
    # 邻接表排序，保证多次运行输出确定
    adj: dict[Hashable, list[Hashable]] = {
        line: sorted(adjacency.get(line, ()), key=_sort_key) for line in lines
    }

    # ------------------------------------------------------------------
    # 1) Kuhn 增广路：match_record[record] = 占用该明细的清单行
    # ------------------------------------------------------------------
    match_record: dict[Hashable, Hashable] = {}

    def augment(line: Hashable, seen: set[Hashable]) -> bool:
        for record in adj.get(line, ()):
            if record in seen:
                continue
            seen.add(record)
            owner = match_record.get(record)
            if owner is None or augment(owner, seen):
                match_record[record] = line
                return True
        return False

    for line in lines:
        if not augment(line, set()):
            # 至少一行无法覆盖 -> 不存在完整对应
            return []

    matching_one: dict[Hashable, Hashable] = {
        line: record for record, line in match_record.items()
    }

    # ------------------------------------------------------------------
    # 2) 构造有向图 D，检测有向环以判定唯一性
    #    非匹配边：L -> R；匹配边：R -> L
    # ------------------------------------------------------------------
    graph: dict[tuple[str, Hashable], list[tuple[str, Hashable]]] = {}
    for line in lines:
        for record in adj[line]:
            left_node = ("L", line)
            right_node = ("R", record)
            if matching_one[line] == record:
                graph.setdefault(right_node, []).append(left_node)
            else:
                graph.setdefault(left_node, []).append(right_node)

    cycle = _find_directed_cycle(graph)
    if not cycle:
        return [matching_one]

    # 环形如 R?, L0, R0, L1, R1, ... ；统一旋转到 L 起点，
    # 则 (L_i, R_i) 是环上的非匹配边，翻转为新匹配
    seq = cycle[:-1] if cycle[0] == cycle[-1] else cycle
    while seq and seq[0][0] != "L":
        seq.append(seq.pop(0))

    matching_two = dict(matching_one)
    for i in range(0, len(seq) - 1, 2):
        left_node, right_node = seq[i], seq[i + 1]
        assert left_node[0] == "L" and right_node[0] == "R"
        matching_two[left_node[1]] = right_node[1]

    _validate_matching(lines, adj, matching_two)
    if matching_two == matching_one:  # 防御性检查：理论上不可能
        return [matching_one]
    return [matching_one, matching_two]


def _find_directed_cycle(
    graph: dict[tuple[str, Hashable], list[tuple[str, Hashable]]],
) -> list[tuple[str, Hashable]] | None:
    """DFS 三色法返回任意一条有向环（含回到起点的重复尾节点），无环返回 None。"""
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[tuple[str, Hashable], int] = {}
    stack: list[tuple[str, Hashable]] = []

    def dfs(node: tuple[str, Hashable]) -> list[tuple[str, Hashable]] | None:
        color[node] = GRAY
        stack.append(node)
        for nxt in graph.get(node, ()):
            state = color.get(nxt, WHITE)
            if state == GRAY:
                idx = stack.index(nxt)
                return stack[idx:] + [nxt]
            if state == WHITE:
                found = dfs(nxt)
                if found is not None:
                    return found
        stack.pop()
        color[node] = BLACK
        return None

    for node in graph:
        if color.get(node, WHITE) == WHITE:
            found = dfs(node)
            if found is not None:
                return found
    return None


def _validate_matching(
    lines: Sequence[Hashable],
    adj: dict[Hashable, list[Hashable]],
    matching: dict[Hashable, Hashable],
) -> None:
    """完整性断言：每行恰好一条边、明细不重复、边真实存在。"""
    if set(matching) != set(lines):
        raise RuntimeError("内部错误：对应未覆盖全部清单行")
    if len(set(matching.values())) != len(lines):
        raise RuntimeError("内部错误：对应中存在重复使用的明细")
    for line, record in matching.items():
        if record not in adj.get(line, ()):
            raise RuntimeError("内部错误：对应使用了不存在的候选边")


def _sort_key(value: Hashable):
    # 明细 id 通常为 int；兜底支持可比较的混合类型
    return (0, value) if isinstance(value, int) else (1, str(value))
