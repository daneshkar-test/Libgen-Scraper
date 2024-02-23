import argparse
import asyncio
import os
import datetime
import aiohttp
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import re
import zipfile

# Function to filter download links
def filter_download_link(href):
    pattern = r"https://download.library.lol/main/\d+/.+?\.pdf"
    if re.search(pattern, href):
        return True
    else:
        return False

async def fetch(session, url):
    user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0")
    try:
        headers = {'User-Agent': user_agent}
        async with session.get(url, headers=headers) as response:
            print(f"Response status code for URL {url}: {response.status}")  # Add this line to print status code
            if response.status == 200:
                response_text = await response.read()
                return response_text.decode('utf-8', 'ignore')
            else:
                print(f"Failed to fetch URL: {url}, status code: {response.status}")
                return None
    except aiohttp.ClientError as e:
        print(f"Error fetching URL: {url}")
        print(e)
        return None


async def download_image(session, image_link, media_subfolder, query):
    filename = os.path.basename(image_link)
    file_path = os.path.join(media_subfolder, filename)

    if os.path.exists(file_path):
        print(f"Image {filename} already exists. Skipping download.")
        return

    try:
        async with session.get(image_link) as response:
            if response.status == 200:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
                print(f"Downloaded image: {filename}")
                insert_image_path(file_path, query)
            else:
                print(f"Failed to download image: {filename}, status code: {response.status}")
    except aiohttp.ClientError as e:
        print(f"Error downloading image: {image_link}")
        print(e)

async def download_book(session, book_link, media_subfolder, query, semaphore):
    print(f"Downloading book: {book_link}")
    async with semaphore:
        try:
            html = await fetch(session, book_link)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                download_link_tag = soup.find('a', href=filter_download_link)
                if download_link_tag:
                    download_url = download_link_tag['href']
                    filename = os.path.basename(download_url)
                    file_path = os.path.join(media_subfolder, filename)

                    if os.path.exists(file_path):
                        print(f"Book {filename} already exists. Skipping download.")
                        return

                    async with session.get(download_url) as response:
                        if response.status == 200:
                            os.makedirs(os.path.dirname(file_path), exist_ok=True)
                            with open(file_path, 'wb') as f:
                                while True:
                                    chunk = await response.content.read(1024)
                                    if not chunk:
                                        break
                                    f.write(chunk)
                            print(f"Downloaded book: {filename}")
                            insert_book_details(filename, query, file_path)
                        else:
                            print(f"Failed to download book: {filename}, status code: {response.status}")
                else:
                    print(f"No download link found for {book_link}")
        except Exception as e:
            print(f"Error downloading book: {book_link}")
            print(e)

async def scrape_page(session, url, media_subfolder, query, semaphore):
    html = await fetch(session, url)
    soup = BeautifulSoup(html, 'html.parser')

    bibtext_links = []
    book_links = []
    book_pages = []

    bibtext_links.extend(
        [f"https://www.libgen.is{a['href']}" for a in soup.find_all('a', href=True) if "bibtex.php" in a['href']]
    )

    for row in soup.find_all('tr', valign='top'):
        image_tag = row.find('img')
        if image_tag:
            image_link = "https://www.libgen.is" + image_tag['src']
            await download_image(session, image_link, media_subfolder, query)

        book_link_tag = row.find('a', href=True)
        if book_link_tag and "md5" in book_link_tag['href']:
            book_pages.append("https://library.lol/main/" + book_link_tag['href'].split("=")[-1])

    for book_page in book_pages:
        html = await fetch(session, book_page)
        soup = BeautifulSoup(html, 'html.parser')
        download_links = soup.find_all('a', href=re.compile(r"https://download.library.lol/main/\d+/.+?\.pdf"))
        for link in download_links:
            book_links.append(link['href'])

    tasks = [download_book(session, book_link, media_subfolder, query, semaphore) for book_link in book_links]
    await asyncio.gather(*tasks)

async def worker(queue, media_subfolder, query, semaphore):
    async with aiohttp.ClientSession() as session:
        while True:
            url, params = await queue.get()
            if url is None:
                print("No URL found")
                break

            full_url = aiohttp.client.URL(url).with_query(params)

            await scrape_page(session, full_url, media_subfolder, query, semaphore)

            queue.task_done()

async def main(semaphore):
    parser = argparse.ArgumentParser(description="Async Web Scraper for Libgen.is")
    parser.add_argument("--query", "-q", type=str, help="Search query")
    parser.add_argument("--open", "-o", type=int, default=0, help="Option for resumed download")
    parser.add_argument("--view", "-v", type=str, default="detailed", help="View type (simple or detailed)")
    parser.add_argument("--results", "-r", type=int, default=25, help="Number of results per page")
    parser.add_argument("--mask", "-m", type=int, default=1, help="Search with mask (0 for Yes, 1 for No)")
    parser.add_argument("--column", "-c", type=str, default="def", help="Column to sort by")
    parser.add_argument("--sort", "-s", type=str, default="def", help="Sorting type")
    parser.add_argument("--sortmode", type=str, default="ASC", help="Sorting mode (ASC or DESC)")
    parser.add_argument("--workers", "-w", type=int, default=5, help="Worker count")
    parser.add_argument("--pages", "-p", type=int, default=10, help="Number of pages to scrape")
    parser.add_argument("--downloads", "-d", type=int, default=5, help="Number of simultaneous downloads")
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return

    num_workers = args.workers
    num_pages = args.pages

    queue = asyncio.Queue()
    for page_number in range(1, num_pages + 1):
        params = {
            "req": args.query,
            "lg_topic": "libgen",
            "open": args.open,
            "view": args.view,
            "res": args.results,
            "phrase": args.mask,
            "column": args.column,
            "sort": args.sort,
            "sortmode": args.sortmode,
            "page": page_number
        }
        url = "https://www.libgen.is/search.php"
        await queue.put((url, params))

    tasks = []
    media_folder = "media"
    if not os.path.exists(media_folder):
        os.makedirs(media_folder)

    media_subfolder = f"media/{args.query}_{datetime.datetime.now().strftime('%Y-%m-%d')}"
    if not os.path.exists(media_subfolder):
        os.makedirs(media_subfolder)

    for _ in range(num_workers):
        task = asyncio.create_task(worker(queue, media_subfolder, args.query, semaphore))
        tasks.append(task)

    if not queue.empty():
        await queue.join()

    for task in tasks:
        task.cancel()

    # Zip the media subfolder
    zip_file_name = f"{args.query}_{datetime.datetime.now().strftime('%Y-%m-%d')}.zip"
    zip_file_path = os.path.join(os.path.dirname(media_subfolder), zip_file_name)
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for root, _, files in os.walk(media_subfolder):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), os.path.join(media_subfolder, '..')))

def create_db():
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS books
                 (id INTEGER PRIMARY KEY,
                 title TEXT,
                 query TEXT,
                 file_path TEXT,
                 image_path TEXT)''')
    conn.commit()
    conn.close()

def insert_book_details(title, query, file_path):
    try:
        conn = sqlite3.connect('books.db')
        df = pd.read_sql_query("SELECT * FROM books", conn)
        conn.close()

        new_data = pd.DataFrame({'title': [title], 'query': [query], 'file_path': [file_path], 'image_path': [None]})
        df = df.append(new_data, ignore_index=True)
        df = df.dropna()
        conn = sqlite3.connect('books.db')
        df.to_sql('books', conn, if_exists='replace', index=False)
        conn.close()
    except Exception as e:
        print(f"Error inserting book details into database: {title}")
        print(e)

def insert_image_path(image_path, query):
    try:
        conn = sqlite3.connect('books.db')
        df = pd.read_sql_query("SELECT * FROM books", conn)
        conn.close()

        df.loc[df['query'] == query, 'image_path'] = image_path
        df = df.dropna()
        conn = sqlite3.connect('books.db')
        df.to_sql('books', conn, if_exists='replace', index=False)
        conn.close()
    except Exception as e:
        print(f"Error inserting image path into database: {image_path}")
        print(e)

if __name__ == "__main__":
    create_db()
    semaphore = asyncio.Semaphore(3)
    asyncio.run(main(semaphore))
