import asyncio
import aiohttp
import asyncmy
import csv
import time
db_config = {"user": "root", "password": "1234", "host": "127.0.0.1", "db": "scraped"}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}
semaphore = asyncio.Semaphore(100)
async def setup_database():
    conn = None
    try:
        conn = await asyncmy.connect(**db_config)
        async with conn.cursor() as cursor:
            await cursor.execute("CREATE DATABASE IF NOT EXISTS scraped;")
            await cursor.execute("USE scraped;")
            await cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS html_data (
                    site VARCHAR(255) PRIMARY KEY,
                    content LONGTEXT
                );
            """
            )
        print("Database setup complete.")
    except Exception as e:
        print(f"Database setup failed: {e}")
        return False
    finally:
        if conn:
            conn.close()
    return True


async def scrape_and_save(site_url, db_pool):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT site FROM html_data WHERE site = %s", (site_url,)
            )
            if await cursor.fetchone():
                print(f"Skipping (already exists): {site_url}")
                return
    async with semaphore:
        async with aiohttp.ClientSession(headers=headers) as session:
            try:
                async with session.get(site_url, timeout=10) as response:
                    response.raise_for_status()
                    content = await response.text()
                    async with db_pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            sql = (
                                "INSERT INTO html_data (site, content) VALUES (%s, %s)"
                            )
                            val = (site_url, content)
                            await cursor.execute(sql, val)
                            await conn.commit()
                            print(f"Successfully scraped: {site_url}")
            except Exception as e:
                print(f"Error scraping {site_url}: {e}")
async def main():
    if not await setup_database():
        return
    try:
        urls_to_scrape = []
        with open("majestic_million.csv", "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)
            urls_to_scrape = [f"https://{row[0]}" for row in reader if row]
        db_pool = await asyncmy.create_pool(**db_config)
        start_time = time.time()
        tasks = [scrape_and_save(url, db_pool) for url in urls_to_scrape]
        await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        duration = end_time - start_time
        print(f"Scraped {len(urls_to_scrape)} sites in {duration:.2f} seconds.")
        print(f"Rate: {len(urls_to_scrape) / duration:.2f} sites/second.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if "db_pool" in locals() and not db_pool.is_closed():
            db_pool.close()
            await db_pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())