#!/usr/bin/env python3
"""
OpenAI Text-to-SQL Program
This program takes a SQL schema and natural language question,
then uses OpenAI to generate the appropriate SQL query.
"""

import openai
import os
from typing import Optional

def setup_openai_client():
    """Initialize OpenAI client with API key from environment variable"""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("Please set OPENAI_API_KEY environment variable")
    
    client = openai.OpenAI(api_key=api_key)
    return client

def text_to_sql(schema: str, question: str, client) -> Optional[str]:
    """
    Convert natural language question to SQL query given a schema
    
    Args:
        schema: SQL CREATE statements defining the database structure
        question: Natural language question about the data
        client: OpenAI client instance
    
    Returns:
        Generated SQL query as string
    """
    
    prompt = f"""Given the following SQL schema:

{schema}

Please generate a SQL query to answer this question:
{question}

Return only the SQL query, no explanation or additional text.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a SQL expert. Generate only valid SQL queries based on the given schema."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.1
        )
        
        sql_query = response.choices[0].message.content.strip()
        return sql_query
    
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return None

def main():
    """Main function to demonstrate text-to-SQL functionality"""
    
    # Example schema for a simple blockchain database
    schema = """
    CREATE TABLE blocks (
        block_height INT PRIMARY KEY,
        block_hash VARCHAR(64) UNIQUE NOT NULL,
        timestamp DATETIME,
        transaction_count INT,
        block_size INT,
        difficulty DECIMAL(20,8)
    );
    
    CREATE TABLE transactions (
        tx_id VARCHAR(64) PRIMARY KEY,
        block_height INT,
        tx_index INT,
        input_count INT,
        output_count INT,
        fee DECIMAL(16,8),
        FOREIGN KEY (block_height) REFERENCES blocks(block_height)
    );
    """
    
    # Test questions
    test_questions = [
        "What is the total number of blocks?",
        "Show me the average transaction count per block",
        "Find the block with the highest difficulty",
        "What are the total fees collected in the last 100 blocks?",
        "Show me blocks with more than 1000 transactions"
    ]
    
    try:
        # Initialize OpenAI client
        client = setup_openai_client()
        
        print("OpenAI Text-to-SQL Demo")
        print("=" * 50)
        print("\nSchema:")
        print(schema)
        print("\n" + "=" * 50)
        
        # Process each test question
        for i, question in enumerate(test_questions, 1):
            print(f"\nQuestion {i}: {question}")
            print("-" * 40)
            
            sql_query = text_to_sql(schema, question, client)
            
            if sql_query:
                print(f"Generated SQL:\n{sql_query}")
            else:
                print("Failed to generate SQL query")
            
            print()
        
        # Interactive mode
        print("\nInteractive Mode (type 'quit' to exit):")
        print("-" * 40)
        
        while True:
            user_question = input("\nEnter your question: ").strip()
            
            if user_question.lower() in ['quit', 'exit', 'q']:
                break
            
            if user_question:
                sql_query = text_to_sql(schema, user_question, client)
                if sql_query:
                    print(f"Generated SQL:\n{sql_query}")
                else:
                    print("Failed to generate SQL query")
    
    except ValueError as e:
        print(f"Setup error: {e}")
        print("\nTo use this program:")
        print("1. Get an OpenAI API key from https://platform.openai.com/")
        print("2. Set environment variable: export OPENAI_API_KEY='your-key-here'")
        print("3. Install openai: pip install openai")
    
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
