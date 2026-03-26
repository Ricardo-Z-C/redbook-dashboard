import asyncio
import aiosqlite
import os

async def check_schema():
    db_path = "database/sqlite_tables.db"
    
    if not os.path.exists(db_path):
        print(f"Database file {db_path} not found.")
        return

    print(f"Checking database schema: {db_path}")
    async with aiosqlite.connect(db_path) as db:
        try:
            print("\n--- Table: xhs_note ---")
            async with db.execute("PRAGMA table_info(xhs_note)") as cursor:
                async for row in cursor:
                    print(row)

            print("\n--- Table: xhs_note_comment ---")
            async with db.execute("PRAGMA table_info(xhs_note_comment)") as cursor:
                async for row in cursor:
                    print(row)
                    
        except Exception as e:
            print(f"Error querying database: {e}")

if __name__ == "__main__":
    asyncio.run(check_schema())
