import os
import json
import requests
from datetime import datetime,timedelta
from airflow.sdk import dag,task
from dotenv import load_dotenv
from airflow import DAG
import pendulum 