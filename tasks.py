from robocorp import workitems
from robocorp.tasks import task
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables

json = JSON()
http = HTTP()
table = Tables()

TRAFFIC_JSON_FILE_PATH = "output/traffic.json"


@task
def produce_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Produces traffic data work items.
    """
    print('Produce')

    url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json"
    http.download(
        url,
        target_file=TRAFFIC_JSON_FILE_PATH,
        overwrite=True
    )

    traffic_data = load_traffic_data()
    filtered_data = filter_and_sort_traffic_data(traffic_data)
    table.write_table_to_csv(filtered_data, './output/filtered_data.csv')

    latest_data = get_latest_data_by_country(filtered_data)
    latest_data = table.create_table(latest_data)
    table.write_table_to_csv(latest_data, './output/latest_data.csv')
    payloads = create_work_item_payloads(latest_data)
    table.write_table_to_csv(table.create_table(payloads), './output/payloads.csv')
    save_work_item_payload(payloads)

@task
def consume_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Consumes traffic data work items.
    """
    print('Consume')

def load_traffic_data():
    json_data = json.load_json_from_file('./output/traffic.json')
    table_from_json = table.create_table(json_data['value'])
    return table_from_json

def filter_and_sort_traffic_data(data):

    avg_rate_key = 'NumericValue'
    max_rate = 5.0

    gender_key = 'Dim1'
    expected_gender = 'BTSX'

    year_key = 'TimeDim'

    table.filter_table_by_column(data, avg_rate_key, '<', max_rate)
    table.filter_table_by_column(data, gender_key, '==', expected_gender)
    # ? Ascending = False
    table.sort_table_by_column(data, year_key, False)
    return data

def get_latest_data_by_country(data):
    """
        Grouping data by Country Code, and gettint the first row of each group.
        Since the data is already sorted by year (descending), it will get the
        latest row in terms of years.
    """
    country_key = 'SpatialDim'
    # ? Returns a list of group tables
    data = table.group_table_by_column(data, country_key)
    latest_country_data = []

    for group in data:
        # ? Returns the first row of the group
        row = table.pop_table_row(group)

        latest_country_data.append(row)

    return latest_country_data

def create_work_item_payloads(data):
    payloads = []

    # ? Generates a list of dictionaries
    for row in data:
        data_row = dict(
            country = row['SpatialDim'],
            year = row['TimeDim'],
            rate = row['NumericValue']
        )

        payloads.append(data_row)
    return payloads

def save_work_item_payload(payloads):
    for payload in payloads:
        # ? We store our payload as a dictionary with the given variable name (traffic_data).
        variables = dict(traffic_data=payload)
        workitems.outputs.create(variables)
