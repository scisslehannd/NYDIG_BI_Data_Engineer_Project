NYDIG Data Engineering Candidate Project

Ian Fitzgerald

June 6, 2021

## Overview

The project was given with two primary objectives:

1. Design a data pipeline to parse raw Bitcoin blockchain data from JSON form and convert it to flat files
2. Create metrics that analyze on-chain activity

I created a simple and lightweight data pipeline that fulfills both objectives. I will discuss the high level design and the reasoning behind various design decisions.

## Design

The design approach for Object 1 can take many different forms, and can easily become quite complicated depending on performance variables. Some factors that were taken into account are: speed, cost, ease-of-use, and ability to scale.

My design approach is fairly simple. First, the &quot;raw&quot; JSON files are dropped into the S3 bucket named &quot;nydig-bi-raw-data.&quot; This triggers the Lambda function &quot;blockchain-analysis&quot; which parses the raw data and converts it to flat pipe-delimited CSV files. It then uploads these files to an output bucket named &quot;nydig-bi-csv-data&quot;. Glue and Athena are configured to query the files in this output bucket.

Lastly, the Lambda functions performs a number of simple calculations on the data to analyze the on-chain activity. If the activity meets certain thresholds, the Lambda triggers a SNS notification.

Figure 1. High-level design architecture

![high-level-architecture](/img/high-level-architecture.png)

Below we discuss some of the major factors that were taken into consideration for the design.

Speed: the project explicitly states to utilize Python code for the data transformation. Therefore absolute speed did not play a large factor in this design approach. If, however, speed were to become a major factor in the future, I would consider abandoning Python and Lambda and instead switch to using Scala or C++ and an always-on EC2.

Cost: using Lambda functions saves on ETL cost and does not require optimizing cloud compute resources.

Ease-of-use: for non-technical users, utilizing Athena allows users to explore the data in a familiar relational database environment. Additionally, Athena makes it easy to connect to front-end data visualization tools. We can extend this project to include a Superset front-end for building data dashboards. Further, for the purposes of this exercise, other AWS options such as Redshift were not seriously considered due to the significant amount of time required to prepare and set up the cluster.

Ability to scale: the current Bitcoin blockchain size is over 300GB and, at current pace, is growing by one gigabyte every few days[1]. A single block on the Bitcoin blockchain will remain capped at 1 megabyte unless the Bitcoin network undergoes a significant technical change. For these two reasons, processing a single block with Lambda is a great solution, as Lambda is able to finish parsing a single block&#39;s JSON file within a few seconds. Further, using Athena for analyzing the aggregate blockchain is also an excellent choice as it has the ability to scale easily to query all blockchain data (300GB).

Table definitions:

![table_definitions](/img/table-defs.png)

## Metrics

The metrics I created for object 2 are relatively simple. When a transaction file is processed by the Lambda, the metrics are emailed via SNS to anyone who is subscribed to the topic.

The metrics are:

The total sum of transactions for the current block.

The total sum of transaction inputs for the current block.

The total sum of transaction outputs for the current block.

The total value of the Bitcoin in.

The total value of the Bitcoin out.

The total sum of fees collected.

These metrics are useful because they are directly correlated to the volume of on the Bitcoin network. For example, we would be able to track the amount of Bitcoin being moved around for a particular block, and the amount of transactions. If there is a low amount of transactions but a high amount of Bitcoin being moved, we might deduce that &quot;whales&quot; are moving a lot of Bitcoin around.

We might also be able to see that the transaction fees are influence by total transaction volume and transaction size. This would be useful for determining when to move Bitcoin on the network.

## Future Improvements

The basic data pipeline and metrics created for this exercise are extremely useful, however we could easily create a more sophisticated pipeline with additional features. Some key improvements include:

1. Creating a mechanism to alert when a new block file doesn&#39;t land on the S3 bucket in an expected time frame. We know that the current block time is around 10 minutes. Therefore, if a block file doesn&#39;t land on the S3 bucket for 30 minutes, we would want to send out an alert to the data team.

1. PagerDuty Integration: it would be beneficial for the data team to integrate PagerDuty, as different alerts could generate different levels of alarm. For example, a small risk alert would notify only one engineer, but if an alert is high risk, PagerDuty could immediately escalate it to the leadership team. Additionally, PagerDuty allows for the data team to create a schedule of on-call engineers.

1. More sophisticated time-series statistics. The current metrics ignore time. It would be beneficial to create metrics that take into account time, and would be able to produce alerts based on the time-series data.
2. Additional metrics on the aggregate data. The existing metrics are analyzing the data for each block. I would like to introduce new metrics that analyze the entire blockchain for patterns such as: Common wallet addresses, Periods of high volume/low volume, Large transactions

1. Data visualization: I would love to add a Superset or Tableau front-end component for analysts to create their own charts/graphs and visualizations.

## References

[1] https://www.statista.com/statistics/647523/worldwide-bitcoin-blockchain-size/