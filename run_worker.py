from dotenv import load_dotenv
from riot.crawler import Crawler

if __name__ == "__main__":
    load_dotenv()
    c = Crawler()
    processed = c.drain(max_items=200)
    print(f"Processed {processed} matches")
