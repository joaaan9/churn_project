import pandas as pd

from collections import defaultdict
from functools import reduce
from utils.config import config
from src.features.functions import BaseFunctions, ApplyFunctions
from transform_data import TransformData


months_agg = [1, 2, 3, 4, 13]
agg = "agg"

features = {"client": [
    # Trading
    (("rn", BaseFunctions.SUM), months_agg, agg),
    (("gross_sales", BaseFunctions.SUM), months_agg, agg),
    (("one_om", BaseFunctions.SUM), months_agg, agg),
    (("booking_dates", BaseFunctions.SUM), months_agg, agg),
    (("min_booking_date", [BaseFunctions.ISNULL, ApplyFunctions.DIFF_DATE_MIN]), months_agg, agg),
    (("max_booking_date", [BaseFunctions.ISNULL, ApplyFunctions.DIFF_DATE_MAX]), months_agg, agg),
    (("searches_total", BaseFunctions.SUM), months_agg, agg),
    (("valuations_total", BaseFunctions.SUM), months_agg, agg),
    (("bookings_total", BaseFunctions.SUM), months_agg, agg),
    (("bookingcancellation", BaseFunctions.SUM), months_agg, agg),
    (("searches_failed", BaseFunctions.SUM), months_agg, agg),
    (("valuations_failed", BaseFunctions.SUM), months_agg, agg),
    (("bookings_failed", BaseFunctions.SUM), months_agg, agg),
    # Cases
    (("bookings_with_cases_ops", BaseFunctions.SUM), months_agg, agg),
    (("bookings_with_cases_sales", BaseFunctions.SUM), months_agg, agg),
    (("bookings_with_cases_finance", BaseFunctions.SUM), months_agg, agg),
    (("bookout", BaseFunctions.SUM), months_agg, agg),
    (("cases_finance_reason_fm", BaseFunctions.SUM), months_agg, agg),
    (("cases_finance_status_solved", BaseFunctions.SUM), months_agg, agg),
    (("cases_ops_status_solved", BaseFunctions.SUM), months_agg, agg),
    (("cases_ops_status_noproceed", BaseFunctions.SUM), months_agg, agg),
    (("cases_sales_status_solved", BaseFunctions.SUM), months_agg, agg),
    # (("bookingcancellationrequest", BaseFunctions.SUM), months_agg, agg),
    (("bookingcancellationandorwaiverrequest", BaseFunctions.SUM), months_agg, agg),
    (("bookingnotfoundorbookingdiscrepancies", BaseFunctions.SUM), months_agg, agg),
    (("bookingpaidatdestination", BaseFunctions.SUM), months_agg, agg),
    (("bookingstatusandbookingreconfirmation", BaseFunctions.SUM), months_agg, agg),
    (("breachofcontract", BaseFunctions.SUM), months_agg, agg),
    (("businessmodelcommission", BaseFunctions.SUM), months_agg, agg),
    (("cancellation", BaseFunctions.SUM), months_agg, agg),
    (("clientnoshow", BaseFunctions.SUM), months_agg, agg),
    (("commercialactionsgestures", BaseFunctions.SUM), months_agg, agg),
    (("commercialrequestsupport", BaseFunctions.SUM), months_agg, agg),
    (("compensationissues", BaseFunctions.SUM), months_agg, agg),
    (("connectivity", BaseFunctions.SUM), months_agg, agg),
    (("contenterror", BaseFunctions.SUM), months_agg, agg),
    (("contentweberrororsystemfailure", BaseFunctions.SUM), months_agg, agg),
    (("contracterror", BaseFunctions.SUM), months_agg, agg),
    (("contractunification", BaseFunctions.SUM), months_agg, agg),
    (("creation", BaseFunctions.SUM), months_agg, agg),
    (("creditcardfraudccf", BaseFunctions.SUM), months_agg, agg),
    (("creditcontrol", BaseFunctions.SUM), months_agg, agg),
    (("dissatisfaction", BaseFunctions.SUM), months_agg, agg),
    (("doesnotcorrespondtoagency", BaseFunctions.SUM), months_agg, agg),
    (("doublechargeserroneouscharges", BaseFunctions.SUM), months_agg, agg),
    (("duplicatedinvoice", BaseFunctions.SUM), months_agg, agg),
    (("feesnegotiationrequired", BaseFunctions.SUM), months_agg, agg),
    (("feesnegotiationrequested", BaseFunctions.SUM), months_agg, agg),
    (("fraudbookings", BaseFunctions.SUM), months_agg, agg),
    (("fraudulentagencyfag", BaseFunctions.SUM), months_agg, agg),
    (("fraudulentbooking", BaseFunctions.SUM), months_agg, agg),
    (("hotelincidentduringstay", BaseFunctions.SUM), months_agg, agg),
    (("hotelchangeterminaltoterminaltransfer", BaseFunctions.SUM), months_agg, agg),
    (("hotelclosed", BaseFunctions.SUM), months_agg, agg),
    (("hotelbedscompanybrandinfo", BaseFunctions.SUM), months_agg, agg),
    (("hotelbedshotelroomtypedifferences", BaseFunctions.SUM), months_agg, agg),
    (("inflightservice", BaseFunctions.SUM), months_agg, agg),
    (("inconveniencesduringrentacarpickupreturn", BaseFunctions.SUM), months_agg, agg),
    (("inconveniencesduringticketpickup", BaseFunctions.SUM), months_agg, agg),
    (("inconveniencesduringticketutilization", BaseFunctions.SUM), months_agg, agg),
    (("massiveoverselling", BaseFunctions.SUM), months_agg, agg),
    (("noshow", BaseFunctions.SUM), months_agg, agg),
    (("overbooking", BaseFunctions.SUM), months_agg, agg),
    (("paymentissue", BaseFunctions.SUM), months_agg, agg),
    (("prepayment", BaseFunctions.SUM), months_agg, agg),
    (("pricematch", BaseFunctions.SUM), months_agg, agg),
    (("ratediscrepancy", BaseFunctions.SUM), months_agg, agg),
    (("ratedisputeorreservationcontracterror", BaseFunctions.SUM), months_agg, agg),
    (("ratesandfees", BaseFunctions.SUM), months_agg, agg),
    (("requestvalidcreditcard", BaseFunctions.SUM), months_agg, agg),
    (("resendvoucher", BaseFunctions.SUM), months_agg, agg),
    (("reservationerrorornotfound", BaseFunctions.SUM), months_agg, agg),
    (("returnoperationproblem", BaseFunctions.SUM), months_agg, agg),
    (("salessupportwebcredentials", BaseFunctions.SUM), months_agg, agg),
    (("servicedescriptioninnacurate", BaseFunctions.SUM), months_agg, agg),
    (("servicenotprovided", BaseFunctions.SUM), months_agg, agg),
    (("stopsales", BaseFunctions.SUM), months_agg, agg),
    (("systemissue", BaseFunctions.SUM), months_agg, agg),
    (("weberrorsystemfailure", BaseFunctions.SUM), months_agg, agg),
    (("groups", BaseFunctions.SUM), months_agg, agg),
    (("extras", BaseFunctions.SUM), months_agg, agg),
    (("hotel_contract", BaseFunctions.SUM), months_agg, agg),
    (("accomocation", BaseFunctions.SUM), months_agg, agg),
    (("compensation", BaseFunctions.SUM), months_agg, agg),
    (("transfer", BaseFunctions.SUM), months_agg, agg),
    (("circuits", BaseFunctions.SUM), months_agg, agg),
    (("totality_service", BaseFunctions.SUM), months_agg, agg),
    (("excursions", BaseFunctions.SUM), months_agg, agg),
    (("specialist_tours", BaseFunctions.SUM), months_agg, agg),
    # Overrides
    (("AMOUNT_OVERRIDE_EUR", BaseFunctions.SUM), months_agg, agg),
    # markup
    (("markup", BaseFunctions.AVG), months_agg, agg),
    (("channel_markup", BaseFunctions.AVG), months_agg, agg),
    # calls
    (("calls_num_on_hold_cases", BaseFunctions.SUM), months_agg, agg),
    (("calls_num_closed_cases", BaseFunctions.SUM), months_agg, agg),
    (("calls_num_other_cases", BaseFunctions.SUM), months_agg, agg),
    (("calls_total_num_cases", BaseFunctions.SUM), months_agg, agg),
    (("calls_avg_case_aging", BaseFunctions.AVG), months_agg, agg),
    (("calls_avg_fcr", BaseFunctions.AVG), months_agg, agg),
    (("calls_avg_answer_time", BaseFunctions.SUM), months_agg, agg),
    (("calls_sum_calls", BaseFunctions.SUM), months_agg, agg),
    (("calls_sum_attended", BaseFunctions.SUM), months_agg, agg),
    (("calls_avg_sla", BaseFunctions.AVG), months_agg, agg),
    (("calls_sum_avrg_aht", BaseFunctions.SUM), months_agg, agg),
], "hotel": [
    (("rn", BaseFunctions.SUM), months_agg, agg),
    (("gross_sales", BaseFunctions.SUM), months_agg, agg),
    (("one_om", BaseFunctions.SUM), months_agg, agg),
    (("gross_commercial_cost", BaseFunctions.SUM), months_agg, agg),
    (("pax", BaseFunctions.SUM), months_agg, agg),
    (("booking_dates", BaseFunctions.SUM), months_agg, agg),
    (("markup", BaseFunctions.AVG), months_agg, agg),
    (("markup", BaseFunctions.AVG), months_agg, "only"),
    (("cpt_p", BaseFunctions.AVG), months_agg, agg),
    (("cpt_all", BaseFunctions.AVG), months_agg, agg),
    (("cpt_all", BaseFunctions.AVG), months_agg, "only"),
    (("searches", BaseFunctions.SUM), months_agg, agg),
    (("searches", BaseFunctions.SUM), months_agg, "only"),
    (("valuations", BaseFunctions.SUM), months_agg, agg),
    (("valuations", BaseFunctions.SUM), months_agg, "only"),
    (("searches_1_hotel", BaseFunctions.SUM), months_agg, agg),
    (("searches_1_hotel", BaseFunctions.SUM), months_agg, "only"),
    (("times_returned_1_hotel", BaseFunctions.SUM), months_agg, agg),
    (("times_returned_1_hotel", BaseFunctions.SUM), months_agg, "only"),
    (("maxirooms_events", BaseFunctions.SUM), months_agg, agg),
    (("min_booking_date", [BaseFunctions.ISNULL, ApplyFunctions.DIFF_DATE_MIN]), months_agg, agg),
    (("max_booking_date", [BaseFunctions.ISNULL, ApplyFunctions.DIFF_DATE_MAX]), months_agg, agg),
    (("hotel_category_group", BaseFunctions.MAX), [1], agg),
    (("ratemix", BaseFunctions.MAX), months_agg, agg),
    (("ratemix", BaseFunctions.MAX), months_agg, "only"),
    (("has_contracts_to_future", BaseFunctions.MAX), [1], agg),
    (("has_some_gnd", BaseFunctions.MAX), [1], agg),
    (("has_gnd", BaseFunctions.MAX), [1], agg),
    (("segmentation", BaseFunctions.MAX), [1], agg),
    (("hotel_destination_segment", BaseFunctions.MAX), [1], agg),
    (("region", BaseFunctions.MAX), [1], agg),
    (("hotel_total_rooms", BaseFunctions.MAX), [1], agg),
    (("days_to_date_from_gnd", BaseFunctions.MAX), [1], agg),
    (("days_to_date_to_gnd", BaseFunctions.MAX), [1], agg),
    (("days_to_max_arrival_date", BaseFunctions.MAX), [1], agg),
    (("gross_commercial_cost_next_slot", BaseFunctions.SUM), [1], agg),
    (("churn_2m", BaseFunctions.SUM), [6, 13], agg),
    (("churn_3m", BaseFunctions.SUM), [6, 13], agg),
    (("churn_4m", BaseFunctions.SUM), [6, 13], agg),
    (("churn_5m", BaseFunctions.SUM), [6, 13], agg),
    (("churn_more_5m", BaseFunctions.SUM), [6, 13], agg),
]}


class function_utils:
    """
    Class for create features, get the list of features important on each case and create more scenarios for making the prediction
    """
    @staticmethod
    def get_list_features():
        # Metric, operacion, meses, agg
        # RN, sum, [1, 2, 18], "only"
        # MARKUP, avg, [2, 5], "agg"
        list_features = list(map(lambda val: [(x, val[0], val[2]) for x in val[1]], features[config.get_var("name")]))
        list_features = reduce(lambda a, b: a + b, list_features)
        list_features = reduce(
            lambda grp, val: grp[val[0], val[2]].append(val[1]) or grp, list_features, defaultdict(list)
        )
        return list_features


    @staticmethod
    def load_data_more_scenarios(df, features, start_time=2, end_time=8, len=1):
        target_metric = config.get_var("target_metric")

        df_times = [i for i in range(start_time, end_time)]
        scenarios = []

        for m in df_times:
            df_copy = df.loc[(df["TIME_SLOT"] >= m - len), :].copy()
            df_copy["TIME_SLOT"] = df_copy["TIME_SLOT"] - m + len
            result = TransformData(
                df_copy,
                break_date=len,
                features_definition_months=features,
            ).transform_data()
            result["SCENARIO_DATE"] = m
            scenarios.append(result)
            print("Done scenario: " + str(m))
            vars = config.get_var("extra")[0]
            print(result.agg({f"{target_metric}_TARGET_PH_{vars['ph']}_CP_{vars['cp']}": "sum", f"FF_{target_metric}_SUM_L1S": "sum"}))

        df_all = pd.concat(scenarios, ignore_index=True)
        return df_all





