from typing import Any

try:  # pragma: no cover - optional dependency
    import kuzu  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    kuzu = None


class KuzuDriverAdapter:
    """Simple adapter to mimic neo4j.AsyncDriver.execute_query using Kùzu."""

    def __init__(self, db_path: str):
        if kuzu is None:  # pragma: no cover - optional dependency
            msg = 'Kùzu is not installed. Install with "pip install kuzu"'
            raise ImportError(msg)
        self.database = kuzu.Database(db_path)
        self.conn = kuzu.AsyncConnection(self.database)

    async def close(self) -> None:
        await self.conn.close()

    async def execute_query(self, query: str, **parameters: Any):
        # neo4j's execute_query accepts database_ and routing_ kwargs which
        # are ignored here
        parameters.pop('database_', None)
        parameters.pop('routing_', None)
        result = await self.conn.execute(query, parameters or None)
        cols = result.get_column_names()
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append({c: v for c, v in zip(cols, row, strict=False)})
        return rows, None, cols
