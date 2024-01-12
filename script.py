from os import path
import subprocess

dir = path.dirname(__file__)

subprocess.call(["python", path.join(dir, "scripts", "start.py")])
print("Initial list downloaded")
subprocess.call(["python", path.join(dir, "scripts", "querying.py")])
print("Parliament database queried")
subprocess.call(["python", path.join(dir, "scripts", "scraper.py")])
print("Profiles scraped")
subprocess.call(["python", path.join(dir, "scripts", "getwiki.py")])
print("Wikidata queried")
subprocess.call(["python", path.join(dir, "scripts", "merger.py")])
print("Files merged")
subprocess.call(["python", path.join(dir, "scripts", "geocoding.py")])
print("Places geocoded")