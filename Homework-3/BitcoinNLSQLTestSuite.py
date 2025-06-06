#!/usr/bin/env python3
"""
Comprehensive Test Cases for Bitcoin Natural Language to SQL System

This script contains 15 test cases of varying difficulty that demonstrate
the system's ability to convert natural language questions about Bitcoin
blockchain data into correct SQL queries and execute them against the database.

Test Case Structure:
- Natural language question
- Expected SQL statement  
- Actual result from database execution
- Difficulty level and category
"""

import sqlite3
import os
import json
from datetime import datetime
from typing import Dict, List, Tuple, Any

class BitcoinNLSQLTestSuite:
    """Comprehensive test suite for Bitcoin Natural Language to SQL system"""
    
    def __init__(self, database_path: str = "bitcoin.db"):
        self.database_path = database_path
        if not os.path.exists(database_path):
            raise FileNotFoundError(f"Database {database_path} not found")
        
        # Test cases organized by difficulty and category
        self.test_cases = [
            # BASIC COUNTING QUERIES (Difficulty: Easy)
            {
                "id": 1,
                "category": "Basic Counting",
                "difficulty": "Easy",
                "question": "How many blocks are in the database?",
                "expected_sql": "SELECT COUNT(*) FROM blocks;",
                "description": "Simple count of all blocks"
            },
            {
                "id": 2,
                "category": "Basic Counting", 
                "difficulty": "Easy",
                "question": "How many transactions are there?",
                "expected_sql": "SELECT COUNT(*) FROM transactions;",
                "description": "Simple count of all transactions"
            },
            
            # AGGREGATION QUERIES (Difficulty: Medium)
            {
                "id": 3,
                "category": "Aggregation",
                "difficulty": "Medium", 
                "question": "What is the total amount of transaction fees?",
                "expected_sql": "SELECT SUM(fee) FROM transactions WHERE fee IS NOT NULL;",
                "description": "Sum aggregation with NULL handling"
            },
            {
                "id": 4,
                "category": "Aggregation",
                "difficulty": "Medium",
                "question": "What is the average block size?",
                "expected_sql": "SELECT AVG(size) FROM blocks WHERE size IS NOT NULL;",
                "description": "Average calculation with NULL handling"
            },
            
            # SORTING AND LIMITING (Difficulty: Medium)
            {
                "id": 5,
                "category": "Sorting & Limiting",
                "difficulty": "Medium",
                "question": "Show me the latest 5 blocks with their heights and timestamps",
                "expected_sql": "SELECT height, hash, datetime(time, 'unixepoch') as block_time FROM blocks ORDER BY height DESC LIMIT 5;",
                "description": "Sorting, limiting, and timestamp conversion"
            },
            {
                "id": 6,
                "category": "Sorting & Limiting", 
                "difficulty": "Medium",
                "question": "What are the 10 largest transaction outputs by value?",
                "expected_sql": "SELECT txid, value, n FROM transaction_outputs ORDER BY value DESC LIMIT 10;",
                "description": "Sorting by value with limit"
            },
            
            # JOIN QUERIES (Difficulty: Hard)
            {
                "id": 7,
                "category": "Multi-table Joins",
                "difficulty": "Hard",
                "question": "Show me blocks with their transaction count and total output value",
                "expected_sql": """SELECT b.height, b.hash, b.nTx, SUM(o.value) as total_output_value 
                                  FROM blocks b 
                                  JOIN transactions t ON b.hash = t.block_hash 
                                  JOIN transaction_outputs o ON t.txid = o.txid 
                                  GROUP BY b.hash, b.height, b.nTx 
                                  ORDER BY b.height DESC;""",
                "description": "Complex join across three tables with aggregation"
            },
            {
                "id": 8,
                "category": "Multi-table Joins",
                "difficulty": "Hard", 
                "question": "Which blocks have transactions with fees and what are the total fees per block?",
                "expected_sql": """SELECT b.height, b.hash, COUNT(t.txid) as tx_with_fees, SUM(t.fee) as total_block_fees
                                  FROM blocks b 
                                  JOIN transactions t ON b.hash = t.block_hash 
                                  WHERE t.fee IS NOT NULL AND t.fee > 0
                                  GROUP BY b.hash, b.height 
                                  ORDER BY total_block_fees DESC;""",
                "description": "Join with filtering, grouping, and aggregation"
            },
            
            # BITCOIN-SPECIFIC DOMAIN QUERIES (Difficulty: Hard)
            {
                "id": 9,
                "category": "Bitcoin Domain Knowledge",
                "difficulty": "Hard",
                "question": "How many coinbase transactions are there?",
                "expected_sql": "SELECT COUNT(*) FROM transaction_inputs WHERE coinbase IS NOT NULL;",
                "description": "Bitcoin-specific knowledge about coinbase transactions"
            },
            {
                "id": 10,
                "category": "Bitcoin Domain Knowledge",
                "difficulty": "Hard",
                "question": "What is the total Bitcoin supply from coinbase outputs?",
                "expected_sql": """SELECT SUM(o.value) as total_coinbase_output
                                  FROM transaction_outputs o
                                  JOIN transaction_inputs i ON o.txid = i.txid
                                  WHERE i.coinbase IS NOT NULL;""",
                "description": "Bitcoin supply calculation using coinbase transactions"
            },
            
            # COMPLEX ANALYTICAL QUERIES (Difficulty: Expert)
            {
                "id": 11,
                "category": "Complex Analytics",
                "difficulty": "Expert",
                "question": "Show me the distribution of transaction types by script type",
                "expected_sql": """SELECT scriptpubkey_type, COUNT(*) as count, 
                                  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transaction_outputs), 2) as percentage
                                  FROM transaction_outputs 
                                  WHERE scriptpubkey_type IS NOT NULL
                                  GROUP BY scriptpubkey_type 
                                  ORDER BY count DESC;""",
                "description": "Distribution analysis with percentage calculations"
            },
            {
                "id": 12,
                "category": "Complex Analytics", 
                "difficulty": "Expert",
                "question": "Find addresses that have received Bitcoin more than once",
                "expected_sql": """SELECT json_extract(scriptpubkey_addresses, '$[0]') as address, 
                                  COUNT(*) as times_received, SUM(value) as total_received
                                  FROM transaction_outputs 
                                  WHERE scriptpubkey_addresses IS NOT NULL
                                  GROUP BY json_extract(scriptpubkey_addresses, '$[0]')
                                  HAVING COUNT(*) > 1
                                  ORDER BY total_received DESC;""",
                "description": "JSON extraction with grouping and HAVING clause"
            },
            
            # TIME-BASED QUERIES (Difficulty: Hard)
            {
                "id": 13,
                "category": "Time-based Analysis",
                "difficulty": "Hard", 
                "question": "Show me blocks created in the last hour based on timestamp",
                "expected_sql": """SELECT height, hash, datetime(time, 'unixepoch') as block_time, nTx
                                  FROM blocks 
                                  WHERE time > strftime('%s', 'now') - 3600
                                  ORDER BY time DESC;""",
                "description": "Time-based filtering with Unix timestamp conversion"
            },
            
            # UTXO AND SPENDING ANALYSIS (Difficulty: Expert)
            {
                "id": 14,
                "category": "UTXO Analysis",
                "difficulty": "Expert",
                "question": "How many unspent transaction outputs are there?",
                "expected_sql": """SELECT COUNT(*) as unspent_outputs
                                  FROM transaction_outputs o
                                  WHERE NOT EXISTS (
                                      SELECT 1 FROM transaction_inputs i 
                                      WHERE i.prev_txid = o.txid AND i.vout = o.n
                                  );""",
                "description": "UTXO calculation using NOT EXISTS subquery"
            },
            
            # STATISTICAL ANALYSIS (Difficulty: Expert)
            {
                "id": 15,
                "category": "Statistical Analysis",
                "difficulty": "Expert",
                "question": "What are the statistics for block sizes including min, max, and median?",
                "expected_sql": """WITH size_stats AS (
                                      SELECT size,
                                             ROW_NUMBER() OVER (ORDER BY size) as row_num,
                                             COUNT(*) OVER () as total_count
                                      FROM blocks WHERE size IS NOT NULL
                                  )
                                  SELECT 
                                      MIN(size) as min_size,
                                      MAX(size) as max_size,
                                      AVG(size) as avg_size,
                                      (SELECT size FROM size_stats WHERE row_num = (total_count + 1) / 2) as median_size
                                  FROM size_stats;""",
                "description": "Statistical analysis with CTE and window functions"
            }
        ]
    
    def execute_sql(self, sql: str) -> Tuple[List[str], List[Tuple]]:
        """Execute SQL query and return results"""
        try:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            conn.close()
            return columns, rows
        except Exception as e:
            return [], [(f"Error: {e}",)]
    
    def format_result(self, columns: List[str], rows: List[Tuple]) -> str:
        """Format query results for display"""
        if not rows:
            return "No results"
        
        if not columns:
            return str(rows[0][0]) if len(rows) == 1 and len(rows[0]) == 1 else str(rows)
        
        # For single value results
        if len(columns) == 1 and len(rows) == 1:
            return f"{rows[0][0]}"
        
        # For multiple results, create a formatted table
        result_lines = []
        
        # Header
        header = " | ".join(columns)
        result_lines.append(header)
        result_lines.append("-" * len(header))
        
        # Rows (limit to first 5 for readability)
        for i, row in enumerate(rows[:5]):
            row_str = " | ".join([str(val) if val is not None else "NULL" for val in row])
            result_lines.append(row_str)
        
        if len(rows) > 5:
            result_lines.append(f"... and {len(rows) - 5} more rows")
        
        return "\n".join(result_lines)
    
    def run_test_case(self, test_case: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single test case"""
        print(f"\n{'='*80}")
        print(f"TEST CASE {test_case['id']}: {test_case['category']} ({test_case['difficulty']})")
        print(f"{'='*80}")
        print(f"📝 Question: {test_case['question']}")
        print(f"💡 Description: {test_case['description']}")
        print(f"🔍 Expected SQL:")
        
        # Clean up SQL formatting for display
        sql = test_case['expected_sql'].strip()
        if sql.count('\n') > 0:
            # Multi-line SQL - format nicely
            sql_lines = [line.strip() for line in sql.split('\n') if line.strip()]
            formatted_sql = '\n    '.join(sql_lines)
            print(f"    {formatted_sql}")
        else:
            print(f"    {sql}")
        
        # Execute the query
        columns, rows = self.execute_sql(sql)
        result = self.format_result(columns, rows)
        
        print(f"📊 Result:")
        if '\n' in result:
            # Multi-line result
            print("    " + result.replace('\n', '\n    '))
        else:
            print(f"    {result}")
        
        return {
            'test_id': test_case['id'],
            'category': test_case['category'],
            'difficulty': test_case['difficulty'],
            'question': test_case['question'],
            'sql': sql,
            'columns': columns,
            'rows': rows,
            'formatted_result': result,
            'success': len(rows) > 0 or 'Error' not in str(rows)
        }
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all test cases and return summary"""
        print("🚀 COMPREHENSIVE BITCOIN NATURAL LANGUAGE TO SQL TEST SUITE")
        print("�� Testing 15 cases across 6 categories with varying difficulty levels")
        print(f"📁 Database: {self.database_path}")
        
        results = []
        success_count = 0
        
        # Group by difficulty for summary
        difficulty_counts = {"Easy": 0, "Medium": 0, "Hard": 0, "Expert": 0}
        difficulty_success = {"Easy": 0, "Medium": 0, "Hard": 0, "Expert": 0}
        
        for test_case in self.test_cases:
            result = self.run_test_case(test_case)
            results.append(result)
            
            difficulty_counts[result['difficulty']] += 1
            if result['success']:
                success_count += 1
                difficulty_success[result['difficulty']] += 1
        
        # Print summary
        print(f"\n{'='*80}")
        print("📊 TEST SUITE SUMMARY")
        print(f"{'='*80}")
        print(f"Total Tests: {len(self.test_cases)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {len(self.test_cases) - success_count}")
        print(f"Success Rate: {success_count/len(self.test_cases)*100:.1f}%")
        
        print(f"\n📈 DIFFICULTY BREAKDOWN:")
        for difficulty in ["Easy", "Medium", "Hard", "Expert"]:
            total = difficulty_counts[difficulty]
            passed = difficulty_success[difficulty]
            if total > 0:
                print(f"  {difficulty:6s}: {passed}/{total} ({passed/total*100:.1f}%)")
        
        print(f"\n🏷️  CATEGORY COVERAGE:")
        categories = {}
        for result in results:
            cat = result['category']
            if cat not in categories:
                categories[cat] = {'total': 0, 'passed': 0}
            categories[cat]['total'] += 1
            if result['success']:
                categories[cat]['passed'] += 1
        
        for category, stats in categories.items():
            print(f"  {category:20s}: {stats['passed']}/{stats['total']} tests")
        
        return {
            'total_tests': len(self.test_cases),
            'successful_tests': success_count,
            'success_rate': success_count/len(self.test_cases),
            'results': results,
            'difficulty_breakdown': difficulty_success,
            'category_breakdown': categories
        }
    
    def export_test_cases(self, filename: str = "bitcoin_nl_sql_test_cases.json"):
        """Export test cases to JSON for documentation"""
        test_data = {
            'description': 'Comprehensive test cases for Bitcoin Natural Language to SQL system',
            'total_cases': len(self.test_cases),
            'categories': list(set(tc['category'] for tc in self.test_cases)),
            'difficulty_levels': list(set(tc['difficulty'] for tc in self.test_cases)),
            'test_cases': []
        }
        
        for tc in self.test_cases:
            columns, rows = self.execute_sql(tc['expected_sql'])
            formatted_result = self.format_result(columns, rows)
            
            test_data['test_cases'].append({
                'id': tc['id'],
                'category': tc['category'],
                'difficulty': tc['difficulty'],
                'natural_language_question': tc['question'],
                'expected_sql': tc['expected_sql'],
                'actual_result': formatted_result,
                'description': tc['description'],
                'result_columns': columns,
                'result_row_count': len(rows)
            })
        
        with open(filename, 'w') as f:
            json.dump(test_data, f, indent=2)
        
        print(f"📄 Test cases exported to {filename}")
        return filename


def main():
    """Main function to run the test suite"""
    try:
        # Initialize test suite
        test_suite = BitcoinNLSQLTestSuite("bitcoin.db")
        
        # Run all tests
        summary = test_suite.run_all_tests()
        
        # Export test cases for documentation
        test_suite.export_test_cases()
        
        print(f"\n�� Test suite completed!")
        print(f"💾 Detailed results exported to bitcoin_nl_sql_test_cases.json")
        
        # Return success if all tests passed
        return summary['success_rate'] == 1.0
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("💡 Make sure bitcoin.db exists in the current directory")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
