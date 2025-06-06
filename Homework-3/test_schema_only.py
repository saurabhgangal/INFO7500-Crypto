#!/usr/bin/env python3
import sqlite3
import os

def test_schema_extraction():
    """Test database schema extraction"""
    print("🧪 Testing database schema extraction...")
    
    if not os.path.exists('bitcoin.db'):
        print("❌ bitcoin.db not found")
        return False
    
    try:
        conn = sqlite3.connect('bitcoin.db')
        cursor = conn.cursor()
        
        schema_parts = []
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"✅ Found {len(tables)} tables: {', '.join(tables)}")
        
        for table in tables:
            # Get table schema
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
            create_statement = cursor.fetchone()[0]
            schema_parts.append(f"\n-- Table: {table}")
            schema_parts.append(create_statement + ";")
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {table} LIMIT 2")
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            
            if rows:
                schema_parts.append(f"-- Columns: {', '.join(columns)}")
                schema_parts.append(f"-- Sample rows: {len(rows)}")
        
        schema = "\n".join(schema_parts)
        print(f"✅ Schema extracted successfully ({len(schema)} characters)")
        
        # Show first few lines
        print("\n📋 Schema preview:")
        print("\n".join(schema.split('\n')[:10]))
        print("...")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Schema extraction failed: {e}")
        return False

if __name__ == "__main__":
    test_schema_extraction()
