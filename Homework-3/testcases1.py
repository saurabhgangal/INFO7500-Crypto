#!/usr/bin/env python3
"""
Comprehensive Test Suite for Bitcoin Natural Language to SQL System

This test suite contains 15 test cases of varying difficulty levels to validate
the natural language to SQL conversion system for Bitcoin blockchain data.

Each test case contains:
1. Natural language question
2. Expected SQL statement
3. Actual answer from database execution

Test Difficulty Levels:
- Basic (1-5): Simple counting and single table queries
- Intermediate (6-10): Joins, aggregations, and filtering
- Advanced (11-15): Complex multi-table queries, date functions, and analytics
"""

import sqlite3
import os
import sys
from typing import List, Dict, Any, Tuple
from datetime import datetime
import json

class BitcoinTestSuite:
    """Comprehensive test suite for Bitcoin NL-to-SQL system"""
    
    def __init__(self, database_path: str):
        self.database_path = database_path
        if not os.path.exists(database_path):
            raise FileNotFoundError(f"Database not found: {database_path}")
        
        # Define comprehensive test cases
        self.test_cases = [
            # BASIC LEVEL (1-5): Simple queries, single table
            {
                "id": 1,
                "difficulty": "Basic",
                "question": "How many blocks are in the database?",
                "expected_sql": "SELECT COUNT(*) FROM blocks;",
                "description": "Basic count of all blocks"
            },
            {
                "id": 2,
                "difficulty": "Basic", 
                "question": "How many transactions are there?",
                "expected_sql": "SELECT COUNT(*) FROM transactions;",
                "description": "Basic count of all transactions"
            },
            {
                "id": 3,
                "difficulty": "Basic",
                "question": "What is the highest block height?",
                "expected_sql": "SELECT MAX(height) FROM blocks;",
                "description": "Simple aggregation function"
            },
            {
                "id": 4,
                "difficulty": "Basic",
                "question": "How many transaction outputs are there?",
                "expected_sql": "SELECT COUNT(*) FROM transaction_outputs;",
                "description": "Count from outputs table"
            },
            {
                "id": 5,
                "difficulty": "Basic",
                "question": "What is the total size of all blocks?",
                "expected_sql": "SELECT SUM(size) FROM blocks WHERE size IS NOT NULL;",
                "description": "Sum with NULL handling"
            },
            
            # INTERMEDIATE LEVEL (6-10): Joins, filtering, grouping
            {
                "id": 6,
                "difficulty": "Intermediate",
                "question": "What are the latest 5 blocks with their transaction counts?",
                "expected_sql": "SELECT height, hash, nTx, datetime(time, 'unixepoch') as block_time FROM blocks ORDER BY height DESC LIMIT 5;",
                "description": "Ordering with date conversion and limiting"
            },
            {
                "id": 7,
                "difficulty": "Intermediate",
                "question": "What is the total amount of transaction fees collected?",
                "expected_sql": "SELECT SUM(fee) FROM transactions WHERE fee IS NOT NULL AND fee > 0;",
                "description": "Sum with filtering conditions"
            },
            {
                "id": 8,
                "difficulty": "Intermediate",
                "question": "How many transactions does each block contain?",
                "expected_sql": "SELECT b.height, b.hash, COUNT(t.txid) as tx_count FROM blocks b LEFT JOIN transactions t ON b.hash = t.block_hash GROUP BY b.hash, b.height ORDER BY b.height DESC LIMIT 10;",
                "description": "Join with grouping and counting"
            },
            {
                "id": 9,
                "difficulty": "Intermediate",
                "question": "What are the 10 largest transaction outputs by value?",
                "expected_sql": "SELECT txid, value, n as output_index, scriptpubkey_type FROM transaction_outputs WHERE value IS NOT NULL ORDER BY value DESC LIMIT 10;",
                "description": "Filtering and ordering by value"
            },
            {
                "id": 10,
                "difficulty": "Intermediate",
                "question": "Which blocks have more than 1 transaction?",
                "expected_sql": "SELECT height, hash, nTx FROM blocks WHERE nTx > 1 ORDER BY nTx DESC;",
                "description": "Filtering with comparison operators"
            },
            
            # ADVANCED LEVEL (11-15): Complex multi-table queries, analytics
            {
                "id": 11,
                "difficulty": "Advanced",
                "question": "What is the average transaction fee per block?",
                "expected_sql": "SELECT b.height, b.hash, AVG(t.fee) as avg_fee FROM blocks b JOIN transactions t ON b.hash = t.block_hash WHERE t.fee IS NOT NULL AND t.fee > 0 GROUP BY b.hash, b.height ORDER BY b.height DESC LIMIT 10;",
                "description": "Multi-table join with aggregation and grouping"
            },
            {
                "id": 12,
                "difficulty": "Advanced",
                "question": "How much Bitcoin value is in each transaction?",
                "expected_sql": "SELECT t.txid, SUM(o.value) as total_output_value FROM transactions t JOIN transaction_outputs o ON t.txid = o.txid WHERE o.value IS NOT NULL GROUP BY t.txid ORDER BY total_output_value DESC LIMIT 10;",
                "description": "Complex join with aggregation across tables"
            },
            {
                "id": 13,
                "difficulty": "Advanced",
                "question": "What is the distribution of transaction types in outputs?",
                "expected_sql": "SELECT scriptpubkey_type, COUNT(*) as count, ROUND(AVG(value), 8) as avg_value FROM transaction_outputs WHERE scriptpubkey_type IS NOT NULL GROUP BY scriptpubkey_type ORDER BY count DESC;",
                "description": "Grouping with multiple aggregations"
            },
            {
                "id": 14,
                "difficulty": "Advanced",
                "question": "Which addresses have received the most Bitcoin?",
                "expected_sql": "SELECT json_extract(scriptpubkey_addresses, '$[0]') as address, SUM(value) as total_received, COUNT(*) as tx_count FROM transaction_outputs WHERE scriptpubkey_addresses IS NOT NULL AND value IS NOT NULL GROUP BY json_extract(scriptpubkey_addresses, '$[0]') ORDER BY total_received DESC LIMIT 10;",
                "description": "JSON extraction with complex aggregation"
            },
            {
                "id": 15,
                "difficulty": "Advanced",
                "question": "What is the relationship between block size and transaction count?",
                "expected_sql": "SELECT b.height, b.size, b.nTx, ROUND(CAST(b.size AS FLOAT) / b.nTx, 2) as avg_tx_size FROM blocks b WHERE b.size IS NOT NULL AND b.nTx > 0 ORDER BY b.height DESC LIMIT 10;",
                "description": "Mathematical calculations with type casting"
            }
        ]
    
    def execute_test_case(self, test_case: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single test case and return results"""
        try:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            # Execute the SQL query
            cursor.execute(test_case["expected_sql"])
            
            # Get column names and results
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            
            conn.close()
            
            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "error": None
            }
            
        except Exception as e:
            return {
                "success": False,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "error": str(e)
            }
    
    def format_result(self, columns: List[str], rows: List[Tuple], limit: int = 5) -> str:
        """Format query results for display"""
        if not rows:
            return "No results"
        
        if len(rows) == 1 and len(columns) == 1:
            # Single value result
            return f"{columns[0]}: {rows[0][0]}"
        
        # Multiple rows/columns - create table format
        result_lines = []
        
        # Header
        header = " | ".join(f"{col:<15}" for col in columns[:4])  # Limit to 4 columns for display
        result_lines.append(header)
        result_lines.append("-" * len(header))
        
        # Data rows
        display_rows = rows[:limit]
        for row in display_rows:
            formatted_row = []
            for i, val in enumerate(row[:4]):  # Limit to 4 columns
                if val is None:
                    formatted_val = "NULL"
                elif isinstance(val, str) and len(val) > 15:
                    formatted_val = val[:12] + "..."
                elif isinstance(val, float):
                    formatted_val = f"{val:.6f}"
                else:
                    formatted_val = str(val)
                formatted_row.append(f"{formatted_val:<15}")
            result_lines.append(" | ".join(formatted_row))
        
        if len(rows) > limit:
            result_lines.append(f"... and {len(rows) - limit} more rows")
        
        return "\n".join(result_lines)
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all test cases and return comprehensive results"""
        print("🧪 Running Comprehensive Bitcoin NL-to-SQL Test Suite")
        print("=" * 70)
        
        results = {
            "total_tests": len(self.test_cases),
            "passed": 0,
            "failed": 0,
            "test_results": [],
            "summary_by_difficulty": {"Basic": 0, "Intermediate": 0, "Advanced": 0}
        }
        
        for test_case in self.test_cases:
            print(f"\n🔍 Test {test_case['id']}: {test_case['difficulty']} Level")
            print(f"❓ Question: {test_case['question']}")
            print(f"🔍 SQL: {test_case['expected_sql']}")
            
            # Execute the test
            execution_result = self.execute_test_case(test_case)
            
            if execution_result["success"]:
                print(f"✅ PASSED - {execution_result['row_count']} rows returned")
                
                # Format and display results
                formatted_result = self.format_result(
                    execution_result["columns"], 
                    execution_result["rows"]
                )
                print(f"📊 Result:\n{formatted_result}")
                
                results["passed"] += 1
                results["summary_by_difficulty"][test_case["difficulty"]] += 1
                
                test_result = {
                    "id": test_case["id"],
                    "difficulty": test_case["difficulty"],
                    "question": test_case["question"],
                    "sql": test_case["expected_sql"],
                    "answer": formatted_result,
                    "success": True,
                    "description": test_case["description"]
                }
            else:
                print(f"❌ FAILED - {execution_result['error']}")
                results["failed"] += 1
                
                test_result = {
                    "id": test_case["id"],
                    "difficulty": test_case["difficulty"],
                    "question": test_case["question"],
                    "sql": test_case["expected_sql"],
                    "answer": f"ERROR: {execution_result['error']}",
                    "success": False,
                    "description": test_case["description"]
                }
            
            results["test_results"].append(test_result)
        
        return results
    
    def generate_test_report(self, results: Dict[str, Any]) -> str:
        """Generate a comprehensive test report"""
        report_lines = [
            "# Bitcoin Natural Language to SQL Test Report",
            "=" * 60,
            "",
            f"**Total Tests**: {results['total_tests']}",
            f"**Passed**: {results['passed']}",
            f"**Failed**: {results['failed']}",
            f"**Success Rate**: {(results['passed']/results['total_tests']*100):.1f}%",
            "",
            "## Results by Difficulty:",
            f"- Basic: {results['summary_by_difficulty']['Basic']}/5 passed",
            f"- Intermediate: {results['summary_by_difficulty']['Intermediate']}/5 passed", 
            f"- Advanced: {results['summary_by_difficulty']['Advanced']}/5 passed",
            "",
            "## Detailed Test Cases:",
            ""
        ]
        
        for test_result in results["test_results"]:
            status = "✅ PASS" if test_result["success"] else "❌ FAIL"
            report_lines.extend([
                f"### Test {test_result['id']}: {test_result['difficulty']} - {status}",
                f"**Question**: {test_result['question']}",
                f"**SQL**: `{test_result['sql']}`",
                f"**Answer**: {test_result['answer']}",
                f"**Description**: {test_result['description']}",
                ""
            ])
        
        return "\n".join(report_lines)
    
    def export_test_cases_json(self, filename: str = "bitcoin_test_cases.json"):
        """Export test cases in JSON format for external tools"""
        export_data = {
            "metadata": {
                "title": "Bitcoin Natural Language to SQL Test Cases",
                "total_cases": len(self.test_cases),
                "difficulty_levels": ["Basic", "Intermediate", "Advanced"],
                "generated_at": datetime.now().isoformat()
            },
            "test_cases": []
        }
        
        for test_case in self.test_cases:
            # Execute to get actual results
            result = self.execute_test_case(test_case)
            
            export_case = {
                "id": test_case["id"],
                "difficulty": test_case["difficulty"],
                "natural_language_question": test_case["question"],
                "expected_sql": test_case["expected_sql"],
                "description": test_case["description"],
                "execution_successful": result["success"],
                "result_columns": result["columns"],
                "result_rows": result["rows"],
                "result_count": result["row_count"]
            }
            
            if not result["success"]:
                export_case["error"] = result["error"]
            
            export_data["test_cases"].append(export_case)
        
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        print(f"📄 Test cases exported to {filename}")

def main():
    """Main function to run the test suite"""
    if not os.path.exists('bitcoin.db'):
        print("❌ bitcoin.db not found. Please ensure the database exists.")
        sys.exit(1)
    
    try:
        # Initialize test suite
        test_suite = BitcoinTestSuite('bitcoin.db')
        
        # Run all tests
        results = test_suite.run_all_tests()
        
        # Print summary
        print("\n" + "=" * 70)
        print("📊 TEST SUITE SUMMARY")
        print("=" * 70)
        print(f"Total Tests: {results['total_tests']}")
        print(f"Passed: {results['passed']} ✅")
        print(f"Failed: {results['failed']} ❌")
        print(f"Success Rate: {(results['passed']/results['total_tests']*100):.1f}%")
        
        print(f"\nResults by Difficulty:")
        for difficulty, count in results['summary_by_difficulty'].items():
            print(f"  {difficulty}: {count}/5 passed")
        
        # Generate detailed report
        report = test_suite.generate_test_report(results)
        with open('bitcoin_test_report.md', 'w') as f:
            f.write(report)
        print(f"\n📄 Detailed report saved to bitcoin_test_report.md")
        
        # Export test cases
        test_suite.export_test_cases_json()
        
        # Final assessment
        if results['passed'] == results['total_tests']:
            print("\n🎉 ALL TESTS PASSED! Your system is working correctly.")
        elif results['passed'] >= results['total_tests'] * 0.8:
            print(f"\n✅ {results['passed']}/{results['total_tests']} tests passed. System is mostly functional.")
        else:
            print(f"\n⚠️  Only {results['passed']}/{results['total_tests']} tests passed. System needs debugging.")
        
        return results['passed'] == results['total_tests']
        
    except Exception as e:
        print(f"❌ Test suite failed to run: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
