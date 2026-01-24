"""
MiniSpark DAG 可视化
====================
生成 RDD 血统的 ASCII 可视化图。
"""

from typing import List, Dict, Set, Tuple
from .rdd import RDD, DependencyType


class DAGVisualizer:
    """
    DAG 可视化器
    
    生成 RDD 血统（lineage）的 ASCII 图。
    """
    
    def __init__(self):
        pass
    
    def print_dag(self, rdd: RDD, title: str = "RDD DAG (Lineage)"):
        """
        打印 RDD 的 DAG 图
        
        从最终 RDD 向上遍历，展示完整的数据流。
        """
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}\n")
        
        # 获取所有 RDD 及其层级
        levels = self._get_levels(rdd)
        
        # 打印 DAG
        self._print_dag_tree(rdd, levels)
        
        print(f"\n{'='*60}\n")
    
    def _get_levels(self, rdd: RDD) -> Dict[int, int]:
        """
        计算每个 RDD 的层级（用于缩进显示）
        
        层级从最终 RDD（0）向上递增。
        """
        levels = {}
        
        def visit(r: RDD, level: int):
            if r.id in levels:
                # 取更大的层级（确保父节点在子节点上方）
                levels[r.id] = max(levels[r.id], level)
            else:
                levels[r.id] = level
            
            for parent in r.parents:
                visit(parent, level + 1)
        
        visit(rdd, 0)
        return levels
    
    def _print_dag_tree(self, rdd: RDD, levels: Dict[int, int]):
        """打印 DAG 树形结构"""
        visited = set()
        
        def print_rdd(r: RDD, indent: int = 0):
            if r.id in visited:
                # 已访问过，只打印引用
                prefix = "  " * indent
                print(f"{prefix}  └── (引用 RDD[{r.id}])")
                return
            
            visited.add(r.id)
            prefix = "  " * indent
            
            # RDD 信息
            cached_marker = " [CACHED]" if r._cached else ""
            partitions_info = f"partitions={r.num_partitions}"
            
            # 格式化 RDD 名称
            rdd_name = r.name
            if "(" in rdd_name:
                # 提取操作名
                op_name = rdd_name.split("(")[0]
            else:
                op_name = rdd_name
            
            print(f"{prefix}RDD[{r.id}] {rdd_name}{cached_marker}")
            print(f"{prefix}  ({partitions_info})")
            
            # 打印依赖关系
            for i, dep in enumerate(r.dependencies):
                parent = dep.parent_rdd
                dep_type = dep.dependency_type.value
                is_last = i == len(r.dependencies) - 1
                
                # 依赖类型标记
                if dep_type == "wide":
                    dep_marker = f"<== SHUFFLE BOUNDARY (shuffle_{dep.shuffle_id})"
                    print(f"{prefix}  |")
                    print(f"{prefix}  | ({dep_type}: {self._get_operation_type(r)})")
                    print(f"{prefix}  | {dep_marker}")
                    print(f"{prefix}  |")
                else:
                    print(f"{prefix}  |")
                    print(f"{prefix}  | ({dep_type}: {self._get_operation_type(r)})")
                    print(f"{prefix}  |")
                
                # 递归打印父 RDD
                print_rdd(parent, indent)
        
        print_rdd(rdd)
    
    def _get_operation_type(self, rdd: RDD) -> str:
        """从 RDD 名称推断操作类型"""
        name = rdd.name.lower()
        
        if "reduceby" in name:
            return "reduceByKey"
        elif "groupby" in name:
            return "groupByKey"
        elif "join" in name:
            return "join"
        elif "flatmap" in name:
            return "flatMap"
        elif "map" in name:
            return "map"
        elif "filter" in name:
            return "filter"
        elif "union" in name:
            return "union"
        elif "textfile" in name:
            return "input"
        elif "parallelize" in name:
            return "input"
        else:
            return "transform"
    
    def get_dag_string(self, rdd: RDD) -> str:
        """获取 DAG 的字符串表示"""
        lines = []
        visited = set()
        
        def visit(r: RDD, prefix: str = ""):
            if r.id in visited:
                return
            visited.add(r.id)
            
            cached = " [CACHED]" if r._cached else ""
            lines.append(f"{prefix}RDD[{r.id}] {r.name}{cached}")
            
            for dep in r.dependencies:
                parent = dep.parent_rdd
                dep_type = dep.dependency_type.value
                
                if dep_type == "wide":
                    lines.append(f"{prefix}  | (wide: shuffle) <== SHUFFLE BOUNDARY")
                else:
                    lines.append(f"{prefix}  | (narrow)")
                
                visit(parent, prefix)
        
        visit(rdd)
        return "\n".join(lines)
    
    def print_lineage_list(self, rdd: RDD):
        """打印简化的血统列表"""
        print(f"\n血统链 (Lineage):")
        print("-" * 40)
        
        lineage = rdd._get_lineage()
        
        for i, r in enumerate(lineage):
            cached = " [CACHED]" if r._cached else ""
            dep_info = ""
            
            if r.dependencies:
                dep = r.dependencies[0]
                if dep.dependency_type == DependencyType.WIDE:
                    dep_info = " <-- SHUFFLE"
            
            print(f"  {i+1}. RDD[{r.id}] {r.name}{cached}{dep_info}")
        
        print("-" * 40)


def print_rdd_dag(rdd: RDD, title: str = "RDD DAG"):
    """便捷函数：打印 RDD DAG"""
    visualizer = DAGVisualizer()
    visualizer.print_dag(rdd, title)


def print_lineage(rdd: RDD):
    """便捷函数：打印血统列表"""
    visualizer = DAGVisualizer()
    visualizer.print_lineage_list(rdd)
