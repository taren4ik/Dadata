import os
import csv
import asyncio
import datetime
import json
from itertools import islice

import httpx
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("API_KEY_base")

URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/json",
}

CONCURRENCY = 2
BATCH_SIZE = 1000
JSON_BATCH_SIZE = 1000

sem = asyncio.Semaphore(CONCURRENCY)

json_buffer = []
json_lock = asyncio.Lock()
JSON_FILENAME = None

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
    "employee_count",
    "finance_income",
    "finance_expense",
    "finance_revenue",
    "finance_debt",
    "finance_penalty",
    "finance_year",
    "smb"
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


async def flush_json_buffer():
    """Сохраняет накопленный буфер в JSON файл"""
    global json_buffer, JSON_FILENAME

    if not json_buffer:
        return

    async with json_lock:

        try:
            with open(JSON_FILENAME, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []


        existing_data.extend(json_buffer)


        with open(JSON_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)


        json_buffer = []
        print(f"💾 JSON сохранён (всего записей: {len(existing_data)})")


async def add_to_json_buffer(data):
    """Добавляет запись в буфер и сохраняет при достижении лимита"""
    global json_buffer

    json_buffer.append(data)

    # Если буфер заполнен - сохраняем
    if len(json_buffer) >= JSON_BATCH_SIZE:
        await flush_json_buffer()


def safe_get(data, key, default=None):
    """
    Безопасное получение значения из словаря.
    Если data = None, возвращает default.
    """
    if data is None:
        return default
    return data.get(key, default)


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


            suggestions = safe_get(data, "suggestions", [])

            row = {
                "inn_query": inn
            }

            if not suggestions:
                return row


            json_entry = {
                "inn_query": inn,
                "full_response": data,
                "processed_at": datetime.datetime.now().isoformat()
            }
            await add_to_json_buffer(json_entry)

            company = suggestions[0].get("data") if suggestions[0] else {}
            if not company:
                return row

            finance = safe_get(company, "finance", {})
            address = safe_get(company, "address", {})
            address_data = safe_get(address, "data", {})
            management = safe_get(company, "management", {})
            name = safe_get(company, "name", {})
            opf = safe_get(company, "opf", {})
            state = safe_get(company, "state", {})

            documents = safe_get(company.get("documents") or {})
            smb = safe_get(documents.get("smb") or {})

            row.update(
                {
                    "address_data_city":
                        safe_get(address_data, "city"),
                    "address_data_federal_district":
                        safe_get(address_data, "federal_district"),
                    "address_data_region":
                        safe_get(address_data, "region"),
                    "address_data_region_iso_code":
                        safe_get(address_data, "region_iso_code"),
                    "address_data_region_kladr_id":
                        safe_get(address_data, "region_kladr_id"),
                    "address_data_source":
                        safe_get(address_data, "source"),
                    "address_unrestricted_value":
                        safe_get(address, "unrestricted_value"),

                    "authorities":
                        safe_get(company, "authorities"),

                    "inn":
                        safe_get(company, "inn"),
                    "kpp":
                        safe_get(company, "kpp"),
                    "ogrn":
                        safe_get(company, "ogrn"),
                    "ogrn_date":
                        safe_get(company, "ogrn_date"),

                    "okato":
                        safe_get(company, "okato"),
                    "okfs":
                        safe_get(company, "okfs"),
                    "okogu":
                        safe_get(company, "okogu"),
                    "okpo":
                        safe_get(company, "okpo"),
                    "oktmo":
                        safe_get(company, "oktmo"),
                    "okved":
                        safe_get(company, "okved"),
                    "okved_type":
                        safe_get(company, "okved_type"),

                    "management_name":
                        safe_get(management, "name"),
                    "management_post":
                        safe_get(management, "post"),

                    "name_full":
                        safe_get(name, "full"),
                    "name_latin":
                        safe_get(name, "latin"),
                    "name_short_with_opf":
                        safe_get(name, "short_with_opf"),

                    "opf_full":
                        safe_get(opf, "full"),
                    "opf_short":
                        safe_get(opf, "short"),
                    "opf_type":
                        safe_get(opf, "type"),

                    "state_actuality_date":
                        safe_get(state, "actuality_date"),
                    "state_registration_date":
                        safe_get(state, "registration_date"),
                    "state_status":
                        safe_get(state, "status"),

                    "type":
                        safe_get(company, "type"),

                    "finance_income":
                        safe_get(finance, "income"),
                    "finance_expense":
                        safe_get(finance, "expense"),
                    "finance_revenue":
                        safe_get(finance, "revenue"),
                    "finance_debt":
                        safe_get(finance, "debt"),
                    "finance_penalty":
                        safe_get(finance, "penalty"),
                    "finance_year":
                        safe_get(finance, "year"),

                    "employee_count":
                        safe_get(company, "employee_count"),

                    "smb":
                        smb.get("category")
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
    global JSON_FILENAME, json_buffer

    processed_count = 0
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")

    # Инициализация JSON файла
    JSON_FILENAME = f"all_responses_{date_str}.json"

    # Создаём пустой JSON файл
    with open(JSON_FILENAME, 'w', encoding='utf-8') as f:
        json.dump([], f, ensure_ascii=False, indent=2)

    with open(
            f"dadata_result_{date_str}.csv",
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
                verify=False
        ) as client:
            for batch in read_inn_batches(
                    r"c:\Projects\Dadata\inns.txt",
                    BATCH_SIZE,
            ):
                batch_rows = await process_batch(
                    client,
                    batch,
                )

                writer.writerows(batch_rows)
                f.flush()

                processed_count += len(batch_rows)

                print(f"Обработано: {processed_count}")

    # Сохраняем остатки буфера
    if json_buffer:
        await flush_json_buffer()
        print("💾 Сохранены последние записи JSON")

    print(f"✅ CSV сохранён: dadata_result_{date_str}.csv")
    print(f"✅ JSON сохранён: {JSON_FILENAME}")
    print("Готово")


if __name__ == "__main__":
    asyncio.run(main())
