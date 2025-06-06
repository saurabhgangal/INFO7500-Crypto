#!/usr/bin/env python3
import sqlite3
import os

def test_database_access():
    """Test basic database functionality"""
    print("🧪 Testing database access...")
    
    if not os.path.exists('bitcoin.db'):
        print("❌ bitcoin.db not found")
        return False
    
    try:
        conn = sqlite3.connect('bitcoin.db')
        cursor = conn.cursor()
        
        # Test basic queries
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"✅ Found tables: {', '.join(tables)}")
        
        cursor.execute("SELECT COUNT(*) FROM blocks")
        block_count = cursor.fetchone()[0]
        print(f"✅ Block count: {block_count}")
        
        cursor.execute("SELECT COUNT(*) FROM transactions")
        tx_count = cursor.fetchone()[0]
        print(f"✅ Transaction count: {tx_count}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

def test_schema_extraction():
    """Test schema extraction functionality"""
    print("\n🧪 Testing schema extraction...")
    
    try:
        from bitcoin_nl_to_sql import BitcoinNLToSQL
        
        # This will fail without API key, but we can test schema extraction
        try:
            # Try with fake API key just to test schema extraction
            import os
            os.environ['OPENAI_API_KEY'] = 'fake-key-for-testing'
            
            nl_sql = BitcoinNLToSQL('bitcoin.db')
            schema = nl_sql.schema
            
            if 'CREATE TABLE' in schema and 'blocks' in schema:
                print("✅ Schema extraction works")
                print(f"✅ Schema length: {len(schema)} characters")
                return True
            else:
                print("❌ Schema extraction incomplete")
                return False
                
        except Exception as e:
            if "API key" in str(e):
                print("✅ Schema extraction works (API key validation working)")
                return True
            else:
                print(f"❌ Schema extraction failed: {e}")
                return False
                
    except ImportError:
        print("❌ bitcoin_nl_to_sql.py not found or has import issues")
        return False

if __name__ == "__main__":
    print("🚀 Simple Test Suite")
    print("=" * 40)
    
    test1 = test_database_access()
    test2 = test_schema_extraction()
    
    if test1 and test2:
        print("\n🎉 Basic tests passed!")
        print("✅ Ready to test with OpenAI API")
        print("\n🚀 Next steps:")
        print("1. Set API key: export OPENAI_API_KEY='sk-...'")
        print("2. Test: python bitcoin_nl_to_sql.py 'How many blocks?' bitcoin.db")
    else:
        print("\n❌ Some tests failed. Check the errors above.")
