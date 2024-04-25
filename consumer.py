import requests
from robocorp import workitems
from robocorp.tasks import task
from RPA.HTTP import HTTP

http = HTTP()

@task
def consume_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Consumes traffic data work items.

    FileAdapter, Output and Input directory has to be defined in the env variables @ the devdata/env.json file
    """
    for item in workitems.inputs:
        traffic_data = item.payload['traffic_data']
        if len(traffic_data['country']) == 3:
            status, return_json = post_traffic_data_to_sales_system(traffic_data)
            if status == 200:
                item.done()
            else:
                item.fail(
                    exception_type="APPLICATION",
                    code="TRAFFIC_DATA_POST_FAILED",
                    message=return_json["message"],
                )
        else:
            item.fail(
                exception_type="BUSINESS",
                code="INVALID_TRAFFIC_DATA",
                message=item.payload
            )

def post_traffic_data_to_sales_system(traffic_data):
    url = 'https://robocorp.com/inhuman-insurance-inc/sales-system-api'
    response = requests.post(url, json=traffic_data)
    # ? raises exceptions between codes 400 and 600
    # response.raise_for_status()
    return response.status_code, response.json()
