from datetime import datetime

# get current date
today = str(datetime.now().date())


process_data_query = """
#############################################
# Baseline Variables #
#############################################
SELECT  id,league_id,localteam_id,visitorteam_id,localteam_position,visitorteam_position,time,date,Y,
 AVG(localteam_score)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS local_goals,
  AVG(visitorteam_score)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS visitor_goals,

#############################################
# Local Variables  #
#############################################
 AVG(l_passes.total)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_passes_total,
  
   AVG(l_passes.accurate)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_passes_accurate,   

   AVG(l_passes.percentage)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_passes_percentage,   


  AVG(l_attack.attacks)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_attack_attacks,

     AVG(l_attack.dangerous_attacks)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_attack_dangerous_attacks,   



  AVG(l_shots.total)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_shots_total,
 AVG(l_shots.ongoal)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_shots_ongoal, 
  AVG(l_shots.blocked)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_shots_blocked,
  AVG(l_shots.offgoal)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_shots_offgoal,
 AVG(l_shots.insidebox)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_shots_insidebox, 
  AVG(l_shots.outsidebox)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_shots_outsidebox,



     AVG(l_fouls)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_fouls,   
  AVG(l_corners)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_corners,   
  AVG(l_offsides)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_offsides,

  AVG(l_possessiontime)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_possessiontime,   
  AVG(l_yellowcards)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_yellowcards,   
  AVG(l_redcards)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_redcards,

       AVG(l_saves)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_saves,   
  AVG(l_substitutions)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_substitutions,   
  AVG(l_goal_kick)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_goal_kick,

       AVG(l_goal_attempts)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_goal_attempts,   
  AVG(l_free_kick)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_free_kick,   
  AVG(l_tackles)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS l_tackles,

#############################################
# Visitor Variables #
#############################################
   AVG(v_passes.total)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_passes_total, 
  
   AVG(v_passes.accurate)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_passes_accurate,   

   AVG(v_passes.percentage)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_passes_percentage,   


  AVG(v_attack.attacks)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_attack_attacks,

     AVG(v_attack.dangerous_attacks)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_attack_dangerous_attacks,   



  AVG(v_shots.total)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_shots_total,
 AVG(v_shots.ongoal)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_shots_ongoal, 
  AVG(v_shots.blocked)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_shots_blocked,
  AVG(v_shots.offgoal)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_shots_offgoal,
 AVG(v_shots.insidebox)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_shots_insidebox, 
  AVG(v_shots.outsidebox)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_shots_outsidebox,



     AVG(v_fouls)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_fouls,   
  AVG(v_corners)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_corners,   
  AVG(v_offsides)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_offsides,

  AVG(v_possessiontime)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_possessiontime,   
  AVG(v_yellowcards)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_yellowcards,   
  AVG(v_redcards)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_redcards,

       AVG(v_saves)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_saves,   
  AVG(v_substitutions)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_substitutions,   
  AVG(v_goal_kick)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_goal_kick,

       AVG(v_goal_attempts)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_goal_attempts,   
  AVG(v_free_kick)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_free_kick,   
  AVG(v_tackles)
  OVER (
    ORDER BY date
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  ) AS v_tackles
#############################################
# Unique ids query #
#############################################
   FROM (SELECT row[OFFSET(0)].* FROM (
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
