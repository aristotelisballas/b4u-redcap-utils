import pandas as pd
from redcap import Project
# from sqlalchemy import create_engine
import os
from typing import List


# PostgreSQL Connection String: postgresql://user:password@host:port/database
# DB_CONNECTION_STR = 'postgresql://username:password@localhost:5432/my_database'
# TARGET_TABLE = 'redcap_records'


def api_url(base):
    base = base.rstrip("/") + "/"
    return base if base.endswith("api/") else base + "api/"


def connect_to_project(url, token):
    return Project(url, token)

# def sync_redcap_to_postgres():
#     try:
#         # 1. Connect to REDCap
#         print("Connecting to REDCap...")
#         project = Project(api_url(REDCAP_API_URL), REDCAP_API_TOKEN)
#
#         # 2. Export records as JSON (returns as list of dicts)
#         print("Exporting data...")
#         data = project.export_records(
#             format_type='json',
#             raw_or_label="label",
#             raw_or_label_headers='label'
#         )
#
#         if not data:
#             print("No data found in REDCap.")
#             return
#
#         # 3. Convert to DataFrame for easy SQL mapping
#         df = pd.DataFrame(data)
#
#         # 4. Connect to PostgreSQL and Load
#         print(f"Loading {len(df)} records into PostgreSQL...")
#         engine = create_engine(DB_CONNECTION_STR)
#
#         # 'replace' will drop the table and recreate it.
#         # Use 'append' if you are adding new rows to an existing schema.
#         df.to_sql(TARGET_TABLE, engine, if_exists='replace', index=False)
#
#         print("Sync complete!")
#
#     except Exception as e:
#         print(f"An error occurred: {e}")


# def get_single_record(record_id: List[str]):
#     """
#     Fetches a specific record from REDCap by its record_id.
#     """
#     try:
#         # records: List of IDs to fetch
#         # format_type: 'json' returns a list of dictionaries
#         project = Project(api_url(REDCAP_API_URL), REDCAP_API_TOKEN)
#
#         record = project.export_records(
#             records=record_id,
#             format_type='json',
#             raw_or_label="label",
#             raw_or_label_headers='label')
#
#         if not record:
#             print(f"Record ID {record_id} not found.")
#             return None
#
#         return record  # Returns a list (usually one item, or multiple if longitudinal)
#
#     except Exception as e:
#         print(f"Error fetching record {record_id}: {e}")
#         return None


# report_data = project.export_report(
#             report_id=str("3"),
#             format_type='json',
#             raw_or_label='label',         # Answers as text labels
#             raw_or_label_headers='label'  # Headers as question text
#         )
#
# project = Project("https://beam.hua.gr/redcap/api/", "F0F5B3C47BDD2526BC03BC4BAF73804D")
#
# metadata = project.export_metadata(format_type='json')
#
# record_id = 323
#
# redcap_answers = {
#     "record_id": record_id,
# }
#
# field_labels = {}
# for field in metadata:
#     field_labels[field['field_name']] = field['field_label']
#
# records = project.export_records(
#     records=[record_id],
#     format_type='json',
#     raw_or_label="label",
#     raw_or_label_headers='label'
# )
#
# print(f"Total instrument instances returned: {len(records)}\n")
# print("=" * 80)
#
# for record in records:
#     instrument = record.get('redcap_repeat_instrument', 'Main Record')
#     instance = record.get('redcap_repeat_instance', '')
#
#     print(f"\nINSTRUMENT: {instrument}")
#     if instance:
#         print(f"   Instance: {instance}")
#     print("-" * 60)
#
#     for field_name, value in record.items():
#         if field_name in ['record_id', 'redcap_repeat_instrument', 'redcap_repeat_instance']:
#             continue
#
#         label = field_labels.get(field_name, field_name)
#
#         print(f"   {field_name}:")
#         print(f"      Label: {label}")
#         print(f"      Value: {value}")
#         print()
#
# print("\n" + "=" * 80)
# print("Done!")


def export_record_with_labels(project, record_id):
    """
    Export REDCap metadata and records for a single record_id
    and return a JSON-serializable structure with labels and values.
    """

    # Get metadata and build field_name -> field_label map
    metadata = project.export_metadata(format_type='json')
    field_labels = {
        field['field_name']: field['field_label']
        for field in metadata
    }

    # Export records
    records = project.export_records(
        records=[record_id],
        format_type='json',
        raw_or_label="label",
        raw_or_label_headers='label'
    )

    # result = {
    #     "record_id": record_id,
    #     "total_instances": len(records),
    #     "instances": []
    # }
    data = []
    for record in records:
        instrument_dict = {}

        instrument = record.get('redcap_repeat_instrument', 'Main Record')
        instance = record.get('redcap_repeat_instance')

        instrument_dict["Record ID"] = record_id
        instrument_dict["Repeat Instrument"] = instrument
        instrument_dict["Repeat Instance"] = instance

        fields = []
        for field_name, value in record.items():
            # Skip system fields
            if field_name in [
                'record_id',
                'redcap_repeat_instrument',
                'redcap_repeat_instance'
            ]:
                continue

            # Filter out empty values (keep 0 or False)
            if value == "" or value is None:
                continue

            fields.append({
                # "field_name": field_name,
                "field_label": field_labels.get(field_name, field_name),
                "value": value
            })

            instrument_dict[field_labels.get(field_name, field_name)] = value
        data.append(instrument_dict)

        # if fields:
        #     result["instances"].append({
        #         "instrument": instrument,
        #         "instance": instance,
        #         "fields": fields
        #     })

    return data


    