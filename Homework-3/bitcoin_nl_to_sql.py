#!/usr/bin/env python3
"""
Bitcoin Natural Language to SQL Query System

This program accepts natural language questions about Bitcoin blockchain data
and converts them to SQL queries using OpenAI's API, then executes them against
the SQLite database.

Usage:
    python bitcoin_nl_to_sql.py "How many blocks are there?" /path/to/bitcoin.db
    python bitcoin_nl_to_sql.py --interactive /path/to/bitcoin.db
"""

import os
import sys
import sqlite3
import argparse
import json
from typing import Dict, Any, List, Tuple, Optional
from openai import OpenAI
from datetime import datetime

class BitcoinNLToSQL:
    """
    Natural Language to SQL converter for Bitcoin blockchain database
    """
    
    def __init__(self, database_path: str, api_key: str = None):
        self.database_path = os.path.abspath(database_path)
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        
        if not self.api_key:
            raise ValueError("OpenAI API key required. Set OPENAI_API_KEY environment variable or pass api_key parameter.")
        
        # Initialize OpenAI client (new API format)
        self.client = OpenAI(api_key=self.api_key)
        
        # Verify database exists and is accessible
        if not os.path.exists(self.database_path):
            raise FileNotFoundError(f"Database file not found: {self.database_path}")
        
        # Extract database schema
        self.schema = self._extract_database_schema()
        
        # System prompt for OpenAI
        self.system_prompt = self._create_system_prompt()
        
    def _extract_database_schema(self) -> str:
        """
        Extract the complete database schema including tables, columns, and relationships
        """
        try:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            schema_parts = []
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                schema_parts.append(f"\n-- Table: {table}")
                
                # Get table schema
                cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
                create_statement = cursor.fetchone()[0]
                schema_parts.append(create_statement + ";")
                
                # Get sample data to help understand the table contents
                try:
                    cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                    columns = [description[0] for description in cursor.description]
                    rows = cursor.fetchall()
                    
                    if rows:
                        schema_parts.append(f"-- Sample data from {table}:")
                        schema_parts.append(f"-- Columns: {', '.join(columns)}")
                        for i, row in enumerate(rows[:2]):  # Show first 2 rows
                            row_str = ', '.join([str(val)[:50] + '...' if isinstance(val, str) and len(str(val)) > 50 else str(val) for val in row])
                            schema_parts.append(f"-- Row {i+1}: {row_str}")
                except Exception as e:
                    schema_parts.append(f"-- Could not get sample data: {e}")
            
            # Get indexes
            schema_parts.append("\n-- Indexes:")
            cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")
            indexes = cursor.fetchall()
            for index_name, index_sql in indexes:
                if index_sql:
                    schema_parts.append(index_sql + ";")
            
            conn.close()
            
            return "\n".join(schema_parts)
            
        except Exception as e:
            raise Exception(f"Failed to extract database schema: {e}")
    
    def _create_system_prompt(self) -> str:
        """
        Create the system prompt with database schema
        """
        return f"""You are a SQL developer that is expert in Bitcoin and you answer natural language questions about the bitcoind database in a sqlite database. You always only respond with SQL statements that are correct.

DATABASE SCHEMA:
{self.schema}

IMPORTANT GUIDELINES:
1. Only return valid SQLite SQL statements
2. Use proper table and column names from the schema above
3. For Bitcoin-specific questions, consider:
   - Block height, hash, time, size, transaction count
   - Transaction inputs (vin) and outputs (vout), fees
   - Addresses and their balances
   - UTXO (unspent transaction outputs)
4. Use appropriate JOINs when data spans multiple tables
5. Include LIMIT clauses for potentially large result sets
6. Use datetime(time, 'unixepoch') to convert Unix timestamps to readable dates
7. Handle NULL values appropriately
8. For aggregations, use proper GROUP BY clauses

EXAMPLE QUERIES:
- "How many blocks?" → SELECT COUNT(*) FROM blocks;
- "Latest 5 blocks" → SELECT height, hash, datetime(time, 'unixepoch') as block_time, nTx FROM blocks ORDER BY height DESC LIMIT 5;
- "Total transaction fees" → SELECT SUM(fee) FROM transactions WHERE fee IS NOT NULL;

RESPONSE FORMAT: Return only the SQL query, no explanations or markdown formatting."""
    
    def query_to_sql(self, natural_language_question: str) -> str:
        """
        Convert natural language question to SQL using OpenAI (new API)
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": natural_language_question}
                ],
                temperature=0.1,  # Low temperature for consistent SQL generation
                max_tokens=500,
                top_p=0.9
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Clean up the SQL (remove any markdown formatting if present)
            sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
            
            return sql_query
            
        except Exception as e:
            raise Exception(f"Failed to generate SQL query: {e}")
    
    def execute_sql(self, sql_query: str) -> Tuple[List[str], List[Tuple]]:
        """
        Execute SQL query against the database and return results
        
        Returns:
            Tuple of (column_names, rows)
        """
        try:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            # Execute the query
            cursor.execute(sql_query)
            
            # Get column names
            column_names = [description[0] for description in cursor.description] if cursor.description else []
            
            # Get results
            rows = cursor.fetchall()
            
            conn.close()
            
            return column_names, rows
            
        except Exception as e:
            raise Exception(f"Failed to execute SQL query: {e}")
    
    def format_results(self, columns: List[str], rows: List[Tuple], limit: int = 50) -> str:
        """
        Format query results for display
        """
        if not rows:
            return "No results found."
        
        if not columns:
            return f"Query executed successfully. Affected rows: {len(rows)}"
        
        # Calculate column widths
        widths = []
        for i, col in enumerate(columns):
            max_width = len(col)
            for row in rows[:limit]:  # Only check displayed rows
                if i < len(row) and row[i] is not None:
                    max_width = max(max_width, len(str(row[i])))
            widths.append(min(max_width, 50))  # Cap at 50 characters
        
        # Format header
        header = " | ".join(col.ljust(width) for col, width in zip(columns, widths))
        separator = "-" * len(header)
        
        # Format rows
        formatted_rows = []
        displayed_rows = rows[:limit]
        
        for row in displayed_rows:
            formatted_row = []
            for i, (value, width) in enumerate(zip(row, widths)):
                if value is None:
                    formatted_value = "NULL"
                elif isinstance(value, str) and len(value) > width:
                    formatted_value = value[:width-3] + "..."
                else:
                    formatted_value = str(value)
                formatted_row.append(formatted_value.ljust(width))
            formatted_rows.append(" | ".join(formatted_row))
        
        result_parts = [header, separator] + formatted_rows
        
        if len(rows) > limit:
            result_parts.append(f"\n... and {len(rows) - limit} more rows")
        
        return "\n".join(result_parts)
    
    def ask_question(self, question: str, show_sql: bool = True) -> Dict[str, Any]:
        """
        Process a natural language question and return the answer
        """
        result = {
            'question': question,
            'sql': None,
            'columns': [],
            'rows': [],
            'formatted_result': '',
            'error': None
        }
        
        try:
            # Step 1: Convert question to SQL
            print(f"🤔 Processing question: {question}")
            sql_query = self.query_to_sql(question)
            result['sql'] = sql_query
            
            if show_sql:
                print(f"🔍 Generated SQL: {sql_query}")
            
            # Step 2: Execute SQL
            columns, rows = self.execute_sql(sql_query)
            result['columns'] = columns
            result['rows'] = rows
            
            # Step 3: Format results
            formatted_result = self.format_results(columns, rows)
            result['formatted_result'] = formatted_result
            
            print(f"✅ Query executed successfully!")
            print(f"📊 Results ({len(rows)} rows):")
            print(formatted_result)
            
        except Exception as e:
            error_msg = str(e)
            result['error'] = error_msg
            print(f"❌ Error: {error_msg}")
        
        return result
    
    def interactive_mode(self):
        """
        Run in interactive mode for continuous questioning
        """
        print("🚀 Bitcoin Database Natural Language Query System")
        print("=" * 60)
        print(f"📁 Database: {self.database_path}")
        print("💡 Ask questions about your Bitcoin blockchain data in natural language!")
        print("💡 Type 'quit', 'exit', or 'q' to exit")
        print("💡 Type 'schema' to see the database schema")
        print("💡 Type 'examples' to see example questions")
        print("=" * 60)
        
        while True:
            try:
                question = input("\n❓ Your question: ").strip()
                
                if question.lower() in ['quit', 'exit', 'q']:
                    print("👋 Goodbye!")
                    break
                
                if question.lower() == 'schema':
                    print("\n📋 Database Schema:")
                    print(self.schema)
                    continue
                
                if question.lower() == 'examples':
                    self._show_examples()
                    continue
                
                if not question:
                    continue
                
                print()  # Add spacing
                self.ask_question(question)
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
    
    def _show_examples(self):
        """Show example questions"""
        examples = [
            "How many blocks are in the database?",
            "What are the latest 5 blocks?",
            "Show me the largest transactions by total output value",
            "What's the total amount of transaction fees?",
            "How many transactions are there?",
            "What's the average block size?",
            "Show me blocks from today",
            "What addresses have received the most Bitcoin?",
            "How many unspent transaction outputs are there?",
            "What's the distribution of transaction types?",
            "Show me the busiest blocks (most transactions)",
            "What's the total Bitcoin supply based on coinbase outputs?"
        ]
        
        print("\n💡 Example Questions:")
        print("=" * 40)
        for i, example in enumerate(examples, 1):
            print(f"{i:2d}. {example}")


def main():
    parser = argparse.ArgumentParser(
        description="Bitcoin Natural Language to SQL Query System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python bitcoin_nl_to_sql.py "How many blocks are there?" bitcoin.db
  python bitcoin_nl_to_sql.py --interactive bitcoin.db
  python bitcoin_nl_to_sql.py "Show me the latest 5 blocks" /path/to/bitcoin.db --api-key sk-...
        """
    )
    
    parser.add_argument('question', nargs='?', help='Natural language question about the Bitcoin database')
    parser.add_argument('database', help='Path to the SQLite database file')
    parser.add_argument('--interactive', '-i', action='store_true', help='Run in interactive mode')
    parser.add_argument('--api-key', help='OpenAI API key (or set OPENAI_API_KEY environment variable)')
    parser.add_argument('--show-sql', action='store_true', default=True, help='Show generated SQL query')
    parser.add_argument('--hide-sql', action='store_true', help='Hide generated SQL query')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.interactive and not args.question:
        parser.error("Either provide a question or use --interactive mode")
    
    # Check if database path is absolute
    if not os.path.isabs(args.database):
        print(f"ℹ️  Converting relative path to absolute: {os.path.abspath(args.database)}")
        args.database = os.path.abspath(args.database)
    
    try:
        # Initialize the system
        nl_to_sql = BitcoinNLToSQL(
            database_path=args.database,
            api_key=args.api_key
        )
        
        if args.interactive:
            # Interactive mode
            nl_to_sql.interactive_mode()
        else:
            # Single question mode
            show_sql = args.show_sql and not args.hide_sql
            result = nl_to_sql.ask_question(args.question, show_sql=show_sql)
            
            # Return appropriate exit code
            sys.exit(0 if result['error'] is None else 1)
    
    except Exception as e:
        print(f"❌ Error initializing system: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
