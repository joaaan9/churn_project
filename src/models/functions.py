from sklearn.metrics import f1_score, roc_auc_score, recall_score, accuracy_score
from src.models.functions_utils import mdl_pred, mdl_fit, split_data
from utils.config import config


def model(df):
    # Get extra config params
    ph = config.get_var("extra")[0]["ph"]
    cp = config.get_var("extra")[0]["cp"]

    # Check column meets criteria and avoid last window
    minimum_value_to_consider_not_churn = 0.005
    df["CHURN_LxS"] = df[f"FF_GROSS_SALES_SUM_L{cp}S"] <= minimum_value_to_consider_not_churn
    df = df[~df["CHURN_LxS"]]

    # Get target fields
    features_fields = []
    targets_fields = []
    for element in df.columns.to_list():
        if element[:3] == "FF_":
            features_fields.append(element)
        else:
            targets_fields.append(element)

    # Split data into train and test
    feat_dev, target_dev, feat_test, target_test = split_data(
        df, f"CHURN_PH_{ph}_CP_{cp}S", targets_fields, 0.3
    )

    # Get imporant variables depending on the model
    important_variables = get_important_variables()

    # Only take important_variables of the data
    feat_dev = feat_dev[important_variables]
    feat_test = feat_test[important_variables]

    # Train model
    mdl = mdl_fit(feat_dev, target_dev)

    # Make some predictions on the datasets
    pred_dev = mdl_pred(mdl, feat_dev, target_dev)
    pred_test = mdl_pred(mdl, feat_test, target_test)
    pred_all = mdl_pred(mdl, df[important_variables], df[f"CHURN_PH_{ph}_CP_{cp}S"])

    return df, mdl, important_variables, pred_dev, pred_test, pred_all


def get_metrics(df, df_pred):
    result = {}
    result["f1_score"] = f1_score(df, df_pred)
    result["roc_auc_score"] = roc_auc_score(df, df_pred)
    result["recall_score"] = recall_score(df, df_pred)
    result["accuracy_score"] = accuracy_score(df, df_pred)
    return result


def prediction(model, df):
    important_variables = get_important_variables()
    return model.predict(df[important_variables])


def get_important_variables():
    model = config.get_var("name")
    if model == "client":
        return [
        "FF_AMOUNT_OVERRIDE_EUR_SUM_L1S",
        "FF_CALLS_AVG_CASE_AGING_MEAN_L13S",
        "FF_GROSS_SALES_SUM_L13S",
        "FF_GROSS_SALES_SUM_L1S",
        "FF_ONE_OM_SUM_L13S",
        "FF_ONE_OM_SUM_L1S",
        "FF_MARKUP_MEAN_L13S",
        "FF_BOOKINGCANCELLATION_SUM_L13S",
        "FF_VALUATIONS_TOTAL_SUM_L13S",
        "FF_SEARCHES_TOTAL_SUM_L13S",
        "FF_SEARCHES_TOTAL_SUM_L1S",
        ]
    else:
        return