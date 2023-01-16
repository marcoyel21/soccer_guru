from datetime import datetime

# get current date
today = str(datetime.now().date())


process_data_query = """
SELECT  id,league_id,localteam_id,visitorteam_id,localteam_position,visitorteam_position,time,date,Y,
 AVG(localteam_score)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS local_goals,
   AVG(visitorteam_score)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 0 FOLLOWING
  ) AS visitor_goals FROM (SELECT row[OFFSET(0)].* FROM (
  SELECT ARRAY_AGG(t ORDER BY created_at DESC LIMIT 1) row
  FROM `matches.historic` t
  GROUP BY id
) );
"""



batch_prediction_query = f"""
SELECT
  *
FROM
  ML.PREDICT(MODEL `matches.production_model`,
    (
SELECT
    league_id,
    localteam_id,
    visitorteam_id,
    localteam_position,
    visitorteam_position,
    local_goals,
    visitor_goals,
    time as time,
    date,
    id
    FROM
      `matches.processed_data`
    WHERE
       date > '{today}'))
"""


train_model_query = f"""
CREATE MODEL `matches.production_model`
OPTIONS(model_type='logistic_reg') AS
SELECT
    Y as label,
    league_id,
    localteam_id,
    visitorteam_id,
    localteam_position,
    visitorteam_position,
    local_goals,
    visitor_goals,
    time as time
FROM
  `matches.processed_data`
WHERE
  date < '{today}';
"""
