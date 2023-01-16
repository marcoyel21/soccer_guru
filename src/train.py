# Retrain-Model
from lib import bqml
from queries.bqueries import *

bqml.delete_model("soccerguru.matches.production_model")
print("Previous model object deleted")

bqml.deploy_model(train_model_query)
print("New model object deployed")
