from dotenv import load_dotenv
from riot.crawler import Crawler

if __name__ == "__main__":
    load_dotenv()
    c = Crawler()

    print("[debug] Starting seeding...")
    try:
        challenger = c.api.get_challenger_entries()
        print(f"[debug] Challenger entries returned: {len(challenger)}")
        if challenger:
            first_entry = challenger[0]
            print(f"[debug] First challenger entry: {first_entry}")
    except Exception as ex:
        print(f"[debug] Error calling challenger endpoint: {ex}")

    inserted = c.seed_from_challenger(limit=10)
    print(f"[debug] Final result: Enqueued {inserted} matches")
