# Airport_Congestion_Prediction_System
To analyze historical flight performance, TSA wait times, TSA throughput, and weather data in order to model airport congestion patterns and recommend optimal passenger arrival times.

# In order to run the pipeline in Production, execute below commands in powershell:
$env:ENV="prod"
python -m scripts.jobs.airport

# -------------------  QUICK NOTES FOR REPORT & PRESENTATION ------------------------
# 1. BEFORE TRANSFORMATION FOR AIRPORT_CLEANED
# row count check (source file vs raw dataset)
# column uniqueness, number of nulls for each column, schema check
# analyze/clean 'city', here's the function I used to analyze/clean it accordingly

% def analyze_city(df):
%     airport_city_extracted_df = df.withColumn(
%         'extracted_state_from_city',
%         F.element_at(F.split(F.col('city'), ', '), -1)
%     )

%     airport_city_extracted_df = airport_city_extracted_df.withColumn(
%         'is_state_same',
%         F.when(F.col('extracted_state_from_city') == F.col('state_code'), 1).otherwise(0)
%     )

%     # get rows where extracted state not same as state_code
%     state_not_match = airport_city_extracted_df.filter(airport_city_extracted_df['is_state_same'] == 0)
%     print("COUNT OF extracted state <> state_code: " + str(state_not_match.count()))     # 3756

%     # from those not same as state_code, get those same as 'country'
%     same_as_country = state_not_match.filter(state_not_match['extracted_state_from_city'] == state_not_match['country'])   # 3748
%     print("COUNT OF unmatched state == country: " + str(same_as_country.count()))

%     # from those not same as state_code, get those not same as 'country'
%     not_same_as_country = state_not_match.filter(state_not_match['extracted_state_from_city'] != state_not_match['country'])   # 8
%     print("COUNT OF unmatched state != country:" + str(not_same_as_country.count()))

%     return not_same_as_country