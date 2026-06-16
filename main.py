from dadata import Dadata
import os
import asyncio
import csv
import httpx
import dotenv


dotenv.load_dotenv()
TOKEN = os.getenv("API_KEY")

URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/json",
}

sem = asyncio.Semaphore(20)


async def find_by_inn(client, inn):
    async with sem:
        try:
            r = await client.post(
                URL,
                json={
                    "query": inn,
                    "branch_type": "MAIN"
                }
            )

            r.raise_for_status()

            suggestions = r.json().get("suggestions", [])

            if suggestions:
                company = suggestions[0]

                return {
                    "inn": inn,
                    "name": company.get("value"),
                    "ogrn": company["data"].get("ogrn"),
                }

            return None

        except Exception as e:
            print(f"Ошибка {inn}: {e}")
            return None


async def main():
    inns = [7716810249, 2635255421]  # список ИНН

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=30,
    ) as client:

        tasks = [
            asyncio.create_task(find_by_inn(client, inn))
            for inn in inns
        ]

        with open(
            "dadata_result.csv",
            "w",
            newline="",
            encoding="utf-8"
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "inn",
                    "name",
                    "ogrn",
                ]
            )

            writer.writeheader()

            count = 0

            for task in asyncio.as_completed(tasks):
                row = await task

                if row:
                    writer.writerow(row)

                count += 1

                if count % 100 == 0:
                    print(f"Обработано: {count}")

    print("Готово")


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

