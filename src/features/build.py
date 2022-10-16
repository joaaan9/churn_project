from transform_data import TransformData
from functions_utils import function_utils


def transform_today_data(df, scenario_date):
    result_today = TransformData(
        df,
        break_date=scenario_date,
        features_definition_months=function_utils.get_list_features()
    ).transform_data()
    result_today["SCENARIO_DATE"] = scenario_date
    return result_today


def transform_scenarios_data(df, scenario_date):
    result_scenarios = function_utils.load_data_more_scenarios(
        df,
        features=function_utils.get_list_features(),
        start_time=(scenario_date + 1),
        end_time=(scenario_date + 5),
        len=scenario_date
    )
    return result_scenarios


def create_features(df):
    """
    Transform data for
    :param df: dataframe on which we are creating the features
    :return:
    """
    res = {}
    res["today"] = transform_today_data(df, 4)
    res["scenarios"] = transform_scenarios_data(df, 4)
    return res


