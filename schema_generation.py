from sqlalchemy import MetaData, Engine
from database_connection import DatabaseConnection
from typing import Dict, Any
import json

class SchemaExtractor:
    def __init__(self, metadata: MetaData, engine: Engine):
        self.metadata = metadata
        self.engine = engine
        # Reflect the database if tables aren't loaded
        if not metadata.tables:
            metadata.reflect(bind=engine)

    def get_column_info(self, column) -> Dict[str, Any]:
        """Extract detailed information about a column."""
        info = {
            "name": column.name,
            "type": str(column.type),
            "nullable": column.nullable,
            "primary_key": column.primary_key,
            "default": str(column.server_default) if column.server_default else None,
            "autoincrement": getattr(column, 'autoincrement', None),
        }

        if hasattr(column, 'foreign_keys') and column.foreign_keys:
            fk = next(iter(column.foreign_keys))
            info["foreign_key"] = {
                "references_table": fk.column.table.name,
                "references_column": fk.column.name
            }

        return info

    def get_relationship_info(self, relationship) -> Dict[str, Any]:
        """Extract information about a relationship."""
        try:
            return {
                "name": relationship.key,
                "target": relationship.mapper.class_.__name__,
                "type": "one_to_many" if relationship.uselist else "many_to_one",
                "back_populates": getattr(relationship, 'back_populates', None),
            }
        except Exception as e:
            return {
                "name": relationship.key,
                "error": str(e)
            }

    def extract_table_schema(self, table) -> Dict[str, Any]:
        """Extract schema information for a single table."""
        schema = {
            "table_name": table.name,
            "columns": [],
            "constraints": [],
            "indexes": []
        }

        # Extract column information
        for column in table.columns:
            schema["columns"].append(self.get_column_info(column))

        # Extract constraints
        for constraint in table.constraints:
            constraint_info = {
                "type": constraint.__class__.__name__,
                "name": constraint.name,
                "columns": [col.name for col in constraint.columns]
            }
            schema["constraints"].append(constraint_info)

        # Extract indexes
        for index in table.indexes:
            index_info = {
                "name": index.name,
                "columns": [col.name for col in index.columns],
                "unique": index.unique
            }
            schema["indexes"].append(index_info)

        return schema

    def extract_full_schema(self) -> Dict[str, Any]:
        """Extract schema information for all tables."""
        schema = {
            "tables": {},
            "metadata": {
                "number_of_tables": len(self.metadata.tables),
                "schema_version": "1.0"
            }
        }

        for table_name, table in self.metadata.tables.items():
            schema["tables"][table_name] = self.extract_table_schema(table)

        return schema

    def generate_rag_context(self) -> str:
        """Generate a natural language description of the schema for RAG."""
        schema = self.extract_full_schema()
        context = []

        context.append("Database Schema Description:")
        context.append(f"This database contains {schema['metadata']['number_of_tables']} tables.\n")

        for table_name, table_schema in schema["tables"].items():
            context.append(f"Table: {table_name}")

            # Describe columns
            context.append("Columns:")
            for column in table_schema["columns"]:
                desc = f"- {column['name']} ({column['type']})"
                if column['primary_key']:
                    desc += " [Primary Key]"
                if not column['nullable']:
                    desc += " [Required]"
                if 'foreign_key' in column:
                    fk = column['foreign_key']
                    desc += f" [References {fk['references_table']}.{fk['references_column']}]"
                context.append(desc)

            # Describe constraints
            if table_schema["constraints"]:
                context.append("\nConstraints:")
                for constraint in table_schema["constraints"]:
                    context.append(f"- {constraint['type']} on columns: {', '.join(constraint['columns'])}")

            # Describe indexes
            if table_schema["indexes"]:
                context.append("\nIndexes:")
                for index in table_schema["indexes"]:
                    unique_str = "UNIQUE " if index['unique'] else ""
                    context.append(f"- {unique_str}Index on columns: {', '.join(index['columns'])}")

            context.append("")  # Empty line between tables

        return "\n".join(context)

    def save_schema(self, filename: str, format: str = 'txt') -> None:
        """Save the schema to a file in the specified format."""
        if format not in ['json', 'txt']:
            raise ValueError("Supported formats are 'json' and 'txt'")

        schema = self.extract_full_schema() if format == 'json' else self.generate_rag_context()

        with open(filename, 'w') as f:
            if format == 'json':
                json.dump(schema, f, indent=2)
            else:
                f.write(schema)