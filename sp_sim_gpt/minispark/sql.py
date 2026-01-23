import csv
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .core import RDD

logger = logging.getLogger(__name__)


@dataclass
class Field:
    name: str
    dtype: str


@dataclass
class Schema:
    fields: List[Field]

    def field_names(self) -> List[str]:
        return [field.name for field in self.fields]


@dataclass
class LogicalPlan:
    source: str
    columns: Optional[List[str]] = None
    predicate: Optional[Callable[[Dict[str, Any]], bool]] = None
    group_by: Optional[str] = None
    agg: Optional[str] = None


class DataFrame:
    """A tiny DataFrame with schema and a simple optimizer."""

    def __init__(
        self,
        rdd: RDD,
        schema: Schema,
        plan: LogicalPlan,
    ) -> None:
        self.rdd = rdd
        self.schema = schema
        self.plan = plan

    def select(self, *cols: str) -> "DataFrame":
        cols_list = list(cols)
        plan = LogicalPlan(
            source=self.plan.source,
            columns=cols_list,
            predicate=self.plan.predicate,
            group_by=self.plan.group_by,
            agg=self.plan.agg,
        )
        return DataFrame(self.rdd, self.schema, plan)

    def where(self, predicate: Callable[[Dict[str, Any]], bool]) -> "DataFrame":
        plan = LogicalPlan(
            source=self.plan.source,
            columns=self.plan.columns,
            predicate=predicate,
            group_by=self.plan.group_by,
            agg=self.plan.agg,
        )
        return DataFrame(self.rdd, self.schema, plan)

    def groupBy(self, column: str) -> "GroupedDataFrame":
        return GroupedDataFrame(self, column)

    def optimized_plan(self) -> LogicalPlan:
        logger.info("Optimizer input plan: %s", self.plan)
        plan = LogicalPlan(
            source=self.plan.source,
            columns=self.plan.columns,
            predicate=self.plan.predicate,
            group_by=self.plan.group_by,
            agg=self.plan.agg,
        )
        logger.info("Optimizer output plan: %s", plan)
        return plan

    def _apply_plan(self, plan: LogicalPlan) -> RDD:
        rdd = self.rdd
        if plan.columns:
            rdd = rdd.map(lambda row: {k: row[k] for k in plan.columns if k in row})
        if plan.predicate:
            rdd = rdd.filter(plan.predicate)
        if plan.group_by and plan.agg:
            rdd = (
                rdd.keyBy(lambda row: row[plan.group_by])
                .map(lambda kv: (kv[0], kv[1][plan.agg]))
                .reduceByKey(lambda a, b: a + b)
            )
        return rdd

    def show(self, driver: "Driver", n: int = 20) -> None:
        plan = self.optimized_plan()
        rdd = self._apply_plan(plan)
        rows = rdd.take(driver, n)
        logger.info("DataFrame.show(%s) output:", n)
        for row in rows:
            print(row)


class GroupedDataFrame:
    def __init__(self, df: DataFrame, column: str) -> None:
        self.df = df
        self.column = column

    def agg(self, agg_func: str) -> DataFrame:
        plan = LogicalPlan(
            source=self.df.plan.source,
            columns=self.df.plan.columns,
            predicate=self.df.plan.predicate,
            group_by=self.column,
            agg=agg_func,
        )
        return DataFrame(self.df.rdd, self.df.schema, plan)


class DataFrameReader:
    def __init__(self, driver: "Driver") -> None:
        self.driver = driver

    def csv(self, path: str, schema: Schema) -> DataFrame:
        with open(path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = [self._cast_row(row, schema) for row in reader]
        rdd = self.driver.parallelize(rows, num_partitions=2)
        plan = LogicalPlan(source=f"csv:{path}")
        return DataFrame(rdd, schema, plan)

    def _cast_row(self, row: Dict[str, str], schema: Schema) -> Dict[str, Any]:
        casted: Dict[str, Any] = {}
        for field in schema.fields:
            value = row.get(field.name)
            if value is None:
                casted[field.name] = None
            elif field.dtype == "int":
                casted[field.name] = int(value)
            elif field.dtype == "float":
                casted[field.name] = float(value)
            else:
                casted[field.name] = value
        return casted
