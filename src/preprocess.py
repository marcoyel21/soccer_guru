# Preprocess data
from lib import bqml
from queries.bqueries import *
bqml.delete_table("soccerguru.matches.processed_data")
bqml.process_data(process_data_query,"soccerguru.matches.processed_data")