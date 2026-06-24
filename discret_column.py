import os
import csv
import asyncio
from itertools import islice

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

FIELDNAMES = [
    "inn",
    "inn_query",
     "address_data_region_kladr_id",
    "address_data_city",
    "address_data_federal_district",
    "address_data_region",
    "address_data_region_iso_code",
    "address_data_source",
    "address_unrestricted_value",
    "authorities",
    "kpp",
    "management_name",
    "management_post",
    "name_full",
    "name_latin",
    "name_short_with_opf",
    "ogrn",
    "ogrn_date",
    "okato",
    "okfs",
    "okogu",
    "okpo",
    "oktmo",
    "okved",
    "okved_type",
    "opf_full",
    "opf_short",
    "opf_type",
    "state_actuality_date",
    "state_registration_date",
    "state_status",
    "type",
    "error",
]


def read_inn_batches(filename, batch_size):
    """
    Get return inn.
    """
    with open(filename, "r", encoding="utf-8") as f:
        while True:
            batch = [
                line.strip()
                for line in islice(f, batch_size)
                if line.strip()
            ]

            if not batch:
                break

            yield batch


async def find_by_inn(client, inn):
    """
    Получить информацию по одному ИНН.
    """
    async with sem:
        try:
            response = await client.post(
                URL,
                json={
                    "query": inn,
                    "branch_type": "MAIN",
                },
            )

            response.raise_for_status()

            data = response.json()
            suggestions = data.get("suggestions", [])

            row = {
                "inn_query": inn
            }

            if not suggestions:
                return row

            company = suggestions[0]["data"]

            address = company.get("address") or {}
            address_data = address.get("data") or {}

            management = company.get("management") or {}
            name = company.get("name") or {}
            opf = company.get("opf") or {}
            state = company.get("state") or {}

            row.update(
                {
                    "address_data_city":
                        address_data.get("city"),
                    "address_data_federal_district":
                        address_data.get("federal_district"),
                    "address_data_region":
                        address_data.get("region"),
                    "address_data_region_iso_code":
                        address_data.get("region_iso_code"),
                    "address_data_region_kladr_id":
                        address_data.get("region_kladr_id"),
                    "address_data_source":
                        address_data.get("source"),
                    "address_unrestricted_value":
                        address.get("unrestricted_value"),

                    "authorities":
                        company.get("authorities"),

                    "inn":
                        company.get("inn"),
                    "kpp":
                        company.get("kpp"),
                    "ogrn":
                        company.get("ogrn"),
                    "ogrn_date":
                        company.get("ogrn_date"),

                    "okato":
                        company.get("okato"),
                    "okfs":
                        company.get("okfs"),
                    "okogu":
                        company.get("okogu"),
                    "okpo":
                        company.get("okpo"),
                    "oktmo":
                        company.get("oktmo"),
                    "okved":
                        company.get("okved"),
                    "okved_type":
                        company.get("okved_type"),

                    "management_name":
                        management.get("name"),
                    "management_post":
                        management.get("post"),

                    "name_full":
                        name.get("full"),
                    "name_latin":
                        name.get("latin"),
                    "name_short_with_opf":
                        name.get("short_with_opf"),

                    "opf_full":
                        opf.get("full"),
                    "opf_short":
                        opf.get("short"),
                    "opf_type":
                        opf.get("type"),

                    "state_actuality_date":
                        state.get("actuality_date"),
                    "state_registration_date":
                        state.get("registration_date"),
                    "state_status":
                        state.get("status"),

                    "type":
                        company.get("type"),
                }
            )

            return row

        except Exception as e:
            print(f"Ошибка {inn}: {e}")

            return {
                "inn_query": inn,
                "error": str(e),
            }


async def process_batch(client, inns_batch):
    """
    Обработать один батч ИНН параллельно.
    """
    tasks = [
        asyncio.create_task(find_by_inn(client, inn))
        for inn in inns_batch
    ]

    rows = []

    for task in asyncio.as_completed(tasks):
        rows.append(await task)

    return rows


async def main():
    processed_count = 0

    with open(
        "dadata_result1.csv",
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=FIELDNAMES,
            restval=""
        )
        writer.writeheader()

        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=30,
        ) as client:

            for batch in read_inn_batches(
                "inns.txt",
                BATCH_SIZE,
            ):
                batch_rows = await process_batch(
                    client,
                    batch,
                )

                writer.writerows(batch_rows)
                f.flush()

                processed_count += len(batch_rows)

                print(
                    f"Обработано: {processed_count}"
                )

    print("Готово")


if __name__ == "__main__":
    asyncio.run(main())