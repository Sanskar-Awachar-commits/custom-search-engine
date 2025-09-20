import asyncio
import aiohttp
import asyncmy
import csv
import time
import random

db_config = {"user": "root", "password": "1234", "host": "127.0.0.1", "db": "scraped"}

BROWSER_HEADERS = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-GB,en;q=0.9",
        "Connection": "keep-alive"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    },
    {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }
]

semaphore = asyncio.Semaphore(500)

async def setup_database():
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
        if "conn" in locals() and conn:
            conn.close()
    return True

async def get_scraped_sites_from_db():
    scraped_sites = set()
    try:
        conn = await asyncmy.connect(**db_config)
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT site FROM html_data;")
            results = await cursor.fetchall()
            for row in results:
                scraped_sites.add(row[0])
        print(f"Found {len(scraped_sites)} sites already scraped.")
    except Exception as e:
        print(f"Failed to retrieve scraped sites: {e}")
    finally:
        if "conn" in locals() and conn:
            conn.close()
    return scraped_sites

async def scrape_site(session, site_url, queue):
    async with semaphore:
        await asyncio.sleep(random.uniform(0.1, 0.5))
        headers = random.choice(BROWSER_HEADERS)
        try:
            async with session.get(site_url, headers=headers, timeout=10, ssl=False) as response:
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                if "text/html" not in content_type:
                    print(f"🚫 Skipping non-HTML content for {site_url}")
                    return
                html_content = await response.text()
                await queue.put({"site": site_url, "content": html_content})
                print(f"✅ Scraped: {site_url}")
        except Exception as e:
            print(f"❌ Error scraping {site_url}: {e}")
            
async def db_writer(queue, db_pool):
    batch = []
    while True:
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1)
            batch.append(item)
            queue.task_done()
        except asyncio.TimeoutError:
            if batch:
                await write_to_db(batch, db_pool)
                batch = []
        if len(batch) >= 100:
            await write_to_db(batch, db_pool)
            batch = []

async def write_to_db(batch, db_pool):
    if not batch:
        return
    values = [(item["site"], item["content"]) for item in batch]
    try:
        async with db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                sql = "INSERT IGNORE INTO html_data (site, content) VALUES (%s, %s)"
                await cursor.executemany(sql, values)
                await conn.commit()
            print(f"Saved {len(batch)} sites to the database.")
    except Exception as e:
        print(f"Failed to save batch to database: {e}")
        
async def main():
    if not await setup_database():
        return
    scraped_sites = await get_scraped_sites_from_db()
    try:
        full_urls = []
        with open("majestic_million.csv", "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  
            full_urls = [f"https://{row[0]}" for row in reader if row]
        urls_to_scrape = [url for url in full_urls if url not in scraped_sites]
        print(f"Starting to scrape {len(urls_to_scrape)} new sites.")
        if not urls_to_scrape:
            print("No new sites to scrape. Exiting.")
            return
        queue = asyncio.Queue()
        db_pool = await asyncmy.create_pool(**db_config, autocommit=False)
        writer_task = asyncio.create_task(db_writer(queue, db_pool))
        start_time = time.time()
        async with aiohttp.ClientSession() as session:
            scraper_tasks = [scrape_site(session, url, queue) for url in urls_to_scrape]
            await asyncio.gather(*scraper_tasks)
        await queue.join()
        writer_task.cancel()
        end_time = time.time()
        duration = end_time - start_time
        print(f"\nScraped {len(urls_to_scrape)} sites in {duration:.2f} seconds.")
        print(f"Rate: {len(urls_to_scrape) / duration:.2f} sites/second.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if "db_pool" in locals() and not db_pool.is_closed():
            db_pool.close()
            await db_pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())