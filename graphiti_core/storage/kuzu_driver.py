from typing import Any

try:  # pragma: no cover - optional dependency
    import kuzu  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    kuzu = None


class KuzuDriverAdapter:
    """Simple adapter to mimic neo4j.AsyncDriver.execute_query using Kùzu."""

    def __init__(self, db_path: str):
        if hasattr(result, '__aiter__'):
            async for row in result:
                rows.append(dict(zip(cols, row, strict=False)))
        else:  # fallback for older versions
            while result.has_next():
                row = result.get_next()
                rows.append(dict(zip(cols, row, strict=False)))
        Args:
            db_path: Path to the Kùzu database file.
        
        Raises:
            ImportError: If the Kùzu library is not installed.
        """
        if kuzu is None:  # pragma: no cover - optional dependency
            msg = 'Kùzu is not installed. Install with "pip install kuzu"'
            raise ImportError(msg)
        self.database = kuzu.Database(db_path)
        self.conn = kuzu.AsyncConnection(self.database)

    async def close(self) -> None:
        """
        Closes the asynchronous connection to the Kùzu database.
        """
        await self.conn.close()

    async def execute_query(self, query: str, **parameters: Any):
        # neo4j's execute_query accepts database_ and routing_ kwargs which
        # are ignored here
        """
        Executes a Cypher-like query asynchronously and returns the results.
        
        Removes unsupported `database_` and `routing_` parameters for compatibility with the neo4j interface. Returns a tuple containing a list of result rows as dictionaries, `None` as a placeholder for summary, and the list of column names.
        
        Args:
            query: The Cypher-like query string to execute.
            **parameters: Query parameters.
        
        Returns:
            A tuple of (rows, None, column_names), where rows is a list of dictionaries mapping column names to values, and column_names is a list of column names.
        """
        parameters.pop('database_', None)
        parameters.pop('routing_', None)
        result = await self.conn.execute(query, parameters or None)
        cols = result.get_column_names()
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append({c: v for c, v in zip(cols, row, strict=False)})
        return rows, None, cols
