import requests

total = 0
page = 1
while True:
    resp = requests.get(
        "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        params={"page": page},
    )
    records = resp.json()
    if not records:
        break
    total += len(records)
    page += 1

print(f"Total records from API: {total}, across {page - 1} pages")
