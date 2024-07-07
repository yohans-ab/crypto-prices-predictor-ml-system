
import hopsworks
import pandas as pd

def push_data_to_feature_store(
        feature_group_name: str,
        feature_group_version: int,
        data: dict,
    )-> None:
    """
    This function writes data to a feature store.
    
    args:
    feature_group_name: str: The name of the feature group to write to.
    feature_group_version: int: The version of the feature group to write to.
    data: dict: The data to write to the feature store.
    
    returns:
    
    None
    """
    # Connect to hopsworks project and api key
    project = hopsworks.login(
        project='yohansab',
        api_key_value='OtvJr9KW4n7QAGjH.gTFNA9nwN8xZ1qj2t7h6zYPSKIfZa0jriikWslkQlALCx2596XF4y00wylcXpYmd'
    )

    # get the feature store
    feature_store = project.get_feature_store()

    # get the feature group
    ohlc_feature_group= feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        description="Feature group containing ohlc data",
        primary_key=["product_id", "timestamp"],
        event_time="timestamp",
        online_enabled=True
    )

    # breakpoint()

    # tgrasnfrom the data in to the pandas dataframe
    data = pd.DataFrame([data])

    # insert data into the feature group
    ohlc_feature_group.insert(data)