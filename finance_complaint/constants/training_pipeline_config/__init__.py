from finance_complaint.constants.training_pipeline_config.data_ingestion import *
from finance_complaint.constants.training_pipeline_config.data_validation import *
import os

PIPELINE_NAME = 'finance-complaint'
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), 'finance_artifact')
