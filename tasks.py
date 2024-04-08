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
    cleaned_data = filter_and_sort_traffic_data(traffic_data)
    table.write_table_to_csv(cleaned_data, './output/cleaned_data.csv')

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
