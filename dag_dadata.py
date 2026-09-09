import asyncio
import os

from datetime import datetime

from airflow.decorators import dag, task
#from airflow.providers.postgres.hooks.postgres import PostgresHook

from psycopg2.extras import execute_values

from lib.dadata_client import run_fetch_batch


POSTGRES_CONN_ID = "postgres_default"

BATCH_SIZE = 1000

@dag(
    dag_id="dadata_db_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dadata", "postgres"],
)
def dadata_db_pipeline():

    @task
    def run_pipeline():

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        conn = pg_hook.get_conn()

        token = os.environ[
            "DADATA_API_KEY"
        ]

        processed_count = 0
        last_inn = None

        try:

            while True:

                with conn.cursor() as cur:

                    if last_inn is None:

                        cur.execute(
                            """
                            SELECT inn
                            FROM source_inns
                            WHERE inn IS NOT NULL
                            ORDER BY inn
                            LIMIT %s
                            """,
                            (
                                BATCH_SIZE,
                            ),
                        )

                    else:

                        cur.execute(
                            """
                            SELECT inn
                            FROM source_inns
                            WHERE inn > %s
                              AND inn IS NOT NULL
                            ORDER BY inn
                            LIMIT %s
                            """,
                            (
                                last_inn,
                                BATCH_SIZE,
                            ),
                        )

                    records = cur.fetchall()

                inns = [
                    str(row[0])
                    for row in records
                ]

                if not inns:
                    break

                results = run_fetch_batch(
                    inns=inns,
                    token=token,
                )

                rows = [
                    item["normalized"]
                    for item in results
                ]

                values = []

                for row in rows:

                    values.append(
                        (
                            row.get("inn"),
                            row.get("inn_query"),

                            row.get(
                                "address_data_region_kladr_id"
                            ),

                            row.get(
                                "address_data_city"
                            ),

                            row.get(
                                "address_data_federal_district"
                            ),

                            row.get(
                                "address_data_region"
                            ),

                            row.get(
                                "address_data_region_iso_code"
                            ),

                            row.get(
                                "address_data_source"
                            ),

                            row.get(
                                "address_unrestricted_value"
                            ),

                            row.get(
                                "authorities"
                            ),

                            row.get("kpp"),

                            row.get(
                                "management_name"
                            ),

                            row.get(
                                "management_post"
                            ),

                            row.get("name_full"),

                            row.get("name_latin"),

                            row.get(
                                "name_short_with_opf"
                            ),

                            row.get("ogrn"),

                            row.get("ogrn_date"),

                            row.get("okato"),
                            row.get("okfs"),
                            row.get("okogu"),
                            row.get("okpo"),
                            row.get("oktmo"),
                            row.get("okved"),
                            row.get("okved_type"),

                            row.get("opf_full"),
                            row.get("opf_short"),
                            row.get("opf_type"),

                            row.get(
                                "state_actuality_date"
                            ),

                            row.get(
                                "state_registration_date"
                            ),

                            row.get(
                                "state_status"
                            ),

                            row.get("type"),

                            row.get("error"),

                            row.get(
                                "employee_count"
                            ),

                            row.get(
                                "finance_income"
                            ),

                            row.get(
                                "finance_expense"
                            ),

                            row.get(
                                "finance_revenue"
                            ),

                            row.get(
                                "finance_debt"
                            ),

                            row.get(
                                "finance_penalty"
                            ),

                            row.get(
                                "finance_year"
                            ),

                            row.get("smb"),
                        )
                    )

                insert_sql = """
                INSERT INTO dadata_company (
                    inn,
                    inn_query,

                    address_data_region_kladr_id,
                    address_data_city,
                    address_data_federal_district,
                    address_data_region,
                    address_data_region_iso_code,
                    address_data_source,
                    address_unrestricted_value,

                    authorities,

                    kpp,

                    management_name,
                    management_post,

                    name_full,
                    name_latin,
                    name_short_with_opf,

                    ogrn,
                    ogrn_date,

                    okato,
                    okfs,
                    okogu,
                    okpo,
                    oktmo,

                    okved,
                    okved_type,

                    opf_full,
                    opf_short,
                    opf_type,

                    state_actuality_date,
                    state_registration_date,
                    state_status,

                    type,

                    error,

                    employee_count,

                    finance_income,
                    finance_expense,
                    finance_revenue,
                    finance_debt,
                    finance_penalty,
                    finance_year,

                    smb
                )
                VALUES %s

                ON CONFLICT (inn_query)
                DO UPDATE SET

                    inn =
                        EXCLUDED.inn,

                    address_data_city =
                        EXCLUDED.address_data_city,

                    address_data_region =
                        EXCLUDED.address_data_region,

                    name_full =
                        EXCLUDED.name_full,

                    name_short_with_opf =
                        EXCLUDED.name_short_with_opf,

                    kpp =
                        EXCLUDED.kpp,

                    ogrn =
                        EXCLUDED.ogrn,

                    okved =
                        EXCLUDED.okved,

                    state_status =
                        EXCLUDED.state_status,

                    finance_income =
                        EXCLUDED.finance_income,

                    finance_expense =
                        EXCLUDED.finance_expense,

                    finance_revenue =
                        EXCLUDED.finance_revenue,

                    finance_year =
                        EXCLUDED.finance_year,

                    smb =
                        EXCLUDED.smb,

                    error =
                        EXCLUDED.error
                """

                with conn.cursor() as cur:

                    execute_values(
                        cur,
                        insert_sql,
                        values,
                        page_size=1000,
                    )

                conn.commit()

                processed_count += len(rows)

                last_inn = inns[-1]

                print(
                    f"Processed: "
                    f"{processed_count}"
                )

        except Exception:

            conn.rollback()

            raise

        finally:

            conn.close()


    run_pipeline()


dadata_db_pipeline()