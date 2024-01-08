import os
import sys
import asyncio
import json
from datetime import datetime

import pandas as pd
from prefect import flow, get_run_logger
from pyensign.events import Event
from pyensign.ensign import Ensign
from river.naive_bayes import MultinomialNB
from river.feature_extraction import BagOfWords
from river.compose import Pipeline
from river import metrics

async def handle_ack(ack):
    ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
    print(ts)

async def handle_nack(nack):
    print(f"Could not commit event {nack.id} with error {nack.code}: {nack.error}")

 
class YelpDataPublisher:
    def __init__(self, topic="river_pipeline"):
        self.topic = topic
        self.ensign = Ensign()

    async def publish(self):
        """
        Read data from the yelp.csv file and publish to river_pipeline topic.
        This can be replaced by a real time streaming source
        Check out https://github.com/rotationalio/data-playground for examples
        """
        # create the topic if it does not exist
        await self.ensign.ensure_topic_exists(self.topic)
        train_df = pd.read_csv(os.path.join("data", "yelp.csv"))
        train_dict = train_df.to_dict("records")
        for record in train_dict:
            event = Event(json.dumps(record).encode("utf-8"), mimetype="application/json")
            await self.ensign.publish(self.topic, event, on_ack=handle_ack, on_nack=handle_nack)


class YelpDataSubscriber:
    """
    The YelpDataSubscriber class reads from the river_pipeline topic and incrementally learns
    from the data until it has learned from all of the instances.  It publishes the precision
    and recall metrics to the river_metrics topic after they are calculated at each step.
    """

    def __init__(self, sub_topic="river_pipeline", pub_topic="river_metrics"):
        self.sub_topic = sub_topic
        self.pub_topic = pub_topic
        self.ensign = Ensign()
        self.initialize_model_and_metrics()

    def initialize_model_and_metrics(self):
        """
        Initialize a river model and set up metrics to evaluate the model as it learns
        """
        self.model = Pipeline(('vectorizer', BagOfWords(lowercase=True)),('nb', MultinomialNB()))
        self.confusion_matrix = metrics.ConfusionMatrix(classes=[0,1])
        self.classification_report = metrics.ClassificationReport()
        self.precision_recall =  metrics.Precision(cm=self.confusion_matrix, pos_val=0) + metrics.Recall(cm=self.confusion_matrix, pos_val=0)

    async def run_model_pipeline(self, event):
        """
        Make a prediction and update metrics based on the predicted value and the actual value
        Incrementally learn/update model based on the actual value
        Continue until "done" message is received
        """
        record = json.loads(event.data)
        y_pred = self.model.predict_one(record["text"])
        if y_pred is not None:
            self.confusion_matrix.update(y_true=record["sentiment"], y_pred=y_pred)
            self.classification_report.update(y_true=record["sentiment"], y_pred=y_pred)
        # the precision and recall won't be great at first, but as the model learns on
        # new data, the scores improve
        print(self.precision_recall)
        pr_list = self.precision_recall.get()
        pr_dict = {"precision": pr_list[0], "recall": pr_list[1]}
        event = Event(json.dumps(pr_dict).encode("utf-8"), mimetype="application/json")
        await self.ensign.publish(self.pub_topic, event, on_ack=handle_ack, on_nack=handle_nack)
        # learn from the train example and update the model
        self.model.learn_one(record["text"], record["sentiment"])

    async def subscribe(self):
        """
        Receive messages from river_pipeline topic
        """

        # ensure that the topic exists or create it if it doesn't
        await self.ensign.ensure_topic_exists(self.sub_topic)
        await self.ensign.ensure_topic_exists(self.pub_topic)

        async for event in self.ensign.subscribe(self.sub_topic):
            await self.run_model_pipeline(event)
        

class MetricsSubscriber:
    """
    The MetricsSubscriber class reads from the river_metrics topic and checks to see
    if the precision and recall have fallen below a specified threshold and prints to screen.
    This code can be extended to update a dashboard and/or send alerts.
    """

    def __init__(self, topic="river_metrics", threshold=0.60, logger=None):
        self.topic = topic
        self.threshold = threshold
        self.logger = logger
        self.ensign = Ensign()

    async def check_metrics(self, event):
        """
        Check precision and recall metrics and print if below threshold
        """
        metric_info = json.loads(event.data)
        precision = metric_info["precision"]
        recall = metric_info["recall"]
        if precision < self.threshold:
            self.logger.warn(f"Precision is below threshold: {precision}")
        if recall < self.threshold:
            self.logger.warn(f"Recall is below threshold: {recall}")

    async def subscribe(self):
        """
        Receive messages from river_train_data topic
        """

        # ensure that the topic exists or create it if it doesn't
        await self.ensign.ensure_topic_exists(self.topic)

        async for event in self.ensign.subscribe(self.topic):
            await self.check_metrics(event)


@flow(log_prints=True)
def run_yelp_data_publisher():
    publisher = YelpDataPublisher()
    asyncio.run(publisher.publish())


@flow(log_prints=True)
def run_yelp_data_subscriber():
    subscriber = YelpDataSubscriber()
    asyncio.run(subscriber.subscribe())


@flow(log_prints=True)
def run_metrics_subscriber():
    logger = get_run_logger()
    subscriber = MetricsSubscriber(logger=logger)
    asyncio.run(subscriber.subscribe())


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "publish":
            run_yelp_data_publisher()
        elif sys.argv[1] == "subscribe":
            run_yelp_data_subscriber()
        elif sys.argv[1] == "metrics":
            run_metrics_subscriber()
        else:
            print("Usage: python river_sentiment_analysis.py [publish|subscribe|metrics]")
    else:
        print("Usage: python river_sentiment_analysis.py [publish|subscribe|metrics]")