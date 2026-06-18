import os
import csv
import json
import asyncio
import httpx
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("API_KEY")

URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/json",
}

CONCURRENCY = 20
BATCH_SIZE = 1000

sem = asyncio.Semaphore(CONCURRENCY)


def flatten(data, parent_key="", sep="_"):
    items = {}

    if isinstance(data, dict):
        for k, v in data.items():
            key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(flatten(v, key, sep))

    elif isinstance(data, list):
        for i, v in enumerate(data):
            key = f"{parent_key}{sep}{i}"
            items.update(flatten(v, key, sep))

    else:
        items[parent_key] = data

    return items


async def find_by_inn(client, inn):
    async with sem:
        try:
            r = await client.post(
                URL,
                json={
                    "query": str(inn),
                    "branch_type": "MAIN"
                }
            )

            r.raise_for_status()

            response = r.json()

            row = {
                "inn_query": str(inn)
            }

            suggestions = response.get("suggestions", [])

            if suggestions:
                row.update(
                    flatten(suggestions[0]["data"])
                )

            return row

        except Exception as e:
            print(f"Ошибка {inn}: {e}")

            return {
                "inn_query": str(inn),
                "error": str(e)
            }


async def process_batch(client, inns_batch):
    tasks = [
        asyncio.create_task(find_by_inn(client, inn))
        for inn in inns_batch
    ]

    rows = []

    for task in asyncio.as_completed(tasks):
        row = await task
        rows.append(row)

    return rows


async def main():
    inns = [
        7716810249,
        2635255421,
        # ...
    ]

    fieldnames = []
    writer = None

    processed_count = 0

    with open(
        "dadata_result.csv",
        "w",
        newline="",
        encoding="utf-8-sig"
    ) as f:

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=30,
        ) as client:

            for start in range(
                0,
                len(inns),
                BATCH_SIZE
            ):
                batch = inns[
                    start:start + BATCH_SIZE
                ]

                batch_rows = await process_batch(
                    client,
                    batch
                )

                batch_fields = {
                    key
                    for row in batch_rows
                    for key in row.keys()
                }

                #
                # Заголовок определяем только по
                # первому батчу из 1000 ИНН
                #
                if writer is None:
                    fieldnames = sorted(batch_fields)

                    writer = csv.DictWriter(
                        f,
                        fieldnames=fieldnames,
                        extrasaction="ignore",
                        restval=""
                    )

                    writer.writeheader()

                writer.writerows(batch_rows)

                #
                # Сбрасываем буфер на диск
                #
                f.flush()

                processed_count += len(batch_rows)

                print(
                    f"Обработано: "
                    f"{processed_count}"
                )

    print("Готово")


if __name__ == "__main__":
    asyncio.run(main())


# for task in asyncio.as_completed(tasks):
#     row = await task
#
#     if row:
#         await conn.execute(
#             """
#             insert into dadata_company
#             (inn, company_name, ogrn)
#             values ($1, $2, $3)
#             on conflict (inn) do update
#             set
#                 company_name = excluded.company_name,
#                 ogrn = excluded.ogrn
#             """,
#             row["inn"],
#             row["name"],
#             row["ogrn"]
#         )


# dadata = Dadata(token)
# result = dadata.suggest("party", "7716810249", branch_type="MAIN")
# result = dadata.find_by_id("party",  "7716810249", branch_type="MAIN")
# print(result)

