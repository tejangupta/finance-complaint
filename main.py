import os
from finance_complaint.constants.environment.variable_key import (AWS_ACCESS_KEY_ID_ENV_KEY,
                                                                  AWS_SECRET_ACCESS_KEY_ENV_KEY)
from dotenv import load_dotenv

import argparse
from finance_complaint.exception import FinanceException
from finance_complaint.pipeline import TrainingPipeline, PredictionPipeline
from finance_complaint.logger import logger
from finance_complaint.config.pipeline.training import FinanceConfig
import sys

load_dotenv()
access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY)
secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY)
# print(access_key_id,secret_access_key)


def start_training(start=False):
    try:
        if not start:
            return None

        print('Training Running')
        TrainingPipeline(FinanceConfig()).start()
    except Exception as e:
        raise FinanceException(e, sys)


def start_prediction(start=False):
    try:
        if not start:
            return None

        print('Prediction started')
        PredictionPipeline().start_batch_prediction()
    except Exception as e:
        raise FinanceException(e, sys)


def main(training_status, prediction_status):
    try:
        start_training(start=training_status)
        start_prediction(start=prediction_status)
    except Exception as e:
        raise FinanceException(e, sys)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--t', default=1, type=int, help='If provided true training will be done else not')
        parser.add_argument('--p', default=0, type=int, help='If provided prediction will be done else not')

        args = parser.parse_args()

        main(training_status=args.t, prediction_status=args.p)
    except Exception as e:
        print(e)
        pass
        logger.exception(FinanceException(e, sys))
