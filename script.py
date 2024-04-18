from os import path
import subprocess

dir = path.dirname(__file__)

print("Downloading initial list...")
subprocess.call(["python", path.join(dir, "scripts", "start.py")])
print("Querying Parliament database...")
subprocess.call(["python", path.join(dir, "scripts", "querying.py")])
print("Scraping profiles...")
subprocess.call(["python", path.join(dir, "scripts", "scraper.py")])
print("Querying Wikidata...")
subprocess.call(["python", path.join(dir, "scripts", "getwiki.py")])
print("Merging files...")
subprocess.call(["python", path.join(dir, "scripts", "merger.py")])
print("Geocoding places...")
subprocess.call(["python", path.join(dir, "scripts", "geocoding.py")])