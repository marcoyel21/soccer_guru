# Batch predictions

from IPython.core.display import HTML

def path_to_image_html(path):
    return '<img src="'+ path + '" width="60" >'
    

    
from google.cloud import bigquery
from lib import bqml
from queries.bqueries import *
import pandas as pd
from lib import etl

import os
path = os.path.dirname(os.path.abspath(__file__))
script_path_html=os.path.join(path,'front_end/index.html')




#Make week predictions
predictions=bqml.predict_next_week(batch_prediction_query)
#Convert to dataframe
df = predictions.to_dataframe()


# make table
body=df[["league_id","localteam_id","visitorteam_id","date","time"]]
probs=df["predicted_label_probs"].apply(pd.Series)
probs_1=probs[0].apply(pd.Series)
probs_1.rename(columns = {'label':'label',"prob":"local win bet"}, inplace = True)

#Extract logos
l_names=[]
l_logos=[]
for i in body['localteam_id']:
    team="teams/"+str(i)
    response=etl.api_call(team)
    name=response['name']
    logo=response['logo_path']
    l_names.append(name)
    l_logos.append(logo)
v_names=[]
v_logos=[]
for i in body['visitorteam_id']:
    team="teams/"+str(i)
    response=etl.api_call(team)
    name=response['name']
    logo=response['logo_path']
    v_names.append(name)
    v_logos.append(logo)

# Create webpage

# Create webpage
context = {'local': l_names, 'l_logo': l_logos,
    'visitor': v_names, 'v_logo': v_logos}
context = pd.DataFrame(data=context)

df_concat = pd.concat([body, context,probs_1], axis=1)
df_concat=df_concat[["league_id","local win bet","l_logo",'local','visitor','v_logo','date','time']]

# dates
df_concat['date'] = pd.to_datetime(df_concat['AdmissionDate'])
df_concat.sort_values(by='date')



leagues=[{"league":"Champions League",
           "league_id":2},
          {"league":"Premier League",
           "league_id":8},
        {"league":"La liga",
           "league_id":564},
        {"league":"Liga Portugal",
           "league_id":462},
        {"league":"Ligue 1",
           "league_id":301},
         {"league":"Bundesliga",
           "league_id":82},
        {"league":"Serie A",
           "league_id":384},
        {"league":"Eredivise",
           "league_id":72},
        {"league":"Europa League",
           "league_id":5}]
tables=""
for i in range(len(leagues)):
    league_name=leagues[i]["league"]
    league_number=str(leagues[i]["league_id"])
    league_header =f"""
    <h2>{league_name}</h2>
    """
    df=df_concat.loc[df_concat["league_id"]==league_number]
    df=df.drop("league_id", axis=1)
    # Formato de imagen a logos
    league_probs=df.to_html(escape=False,
                   formatters=dict(l_logo=path_to_image_html,v_logo=path_to_image_html))

    tables=tables+league_header+league_probs
    
    

# Creo html
template= """
<!-- index.html -->

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Am I HTML already?</title>
    <link rel="stylesheet" href="https://cdn.simplecss.org/simple.css">
    <link rel="stylesheet" href="style.css">

</head>
<body>
<h1>Soccerguru, weekend bets!</h1>
<p>Probability of local team winning on major european leagues.</p>
"""


end_html="""
</body>
</html>
"""
index=template+tables+end_html
with open(script_path_html, 'w+') as fh:
    fh.write(index)
    
