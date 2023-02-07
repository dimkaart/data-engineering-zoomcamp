## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770
* 766,792
* 299,234
* 822,132

## Answer 1. Load January 2020 data
The green taxi data set of January 2020 has `447,770` rows. The code for the task can be seen in `etl_web_to_gcs_week2hw_Q1.py`.

The CLI execution for the deployment was: 
`prefect deployment build ./etl_web_to_gcs_week2hw_Q5.py:etl_web_to_gcs -n "ETL_HW_Q5" -a`


## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`


## Answer 2. Scheduling with Cron
The structure of cron is as follows: `minute hour day(of month) month day(of week)`
Therefore, to create a deployment to run on the first of every month at 5am is `0 5 1 * *`. This can be set up within the `etl_parent_flow-deployment.yaml` under the parameter schedule or directly from the CLI with the formulation:

`prefect deployment build etl_web_to_gcs_week2hw_Q2.py:etl_web_to_gcs -n "ETL_HW_Q2" --cron "0 5 1 * *" -a`

The result is:
![alt text](../../images/flow_question2.png)

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920
- 12,282,990
- 27,235,753
- 11,338,483


## Answer 3. Loading data to BigQuery

The flow code processed in total `14,851,920` rows. The code can be seen in `etl_gcs_to_bq_week2hw_Q3.py` and the CLI execution was `prefect deployment build etl_gcs_to_bq_week2hw_Q3.py:etl_parent_flow -n "ETL_HW_Q3" -a`. The logs of the flow are the following ones:
![alt text](../../images/flow_question3.png)

## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225

## Answer 4. Github Storage Block

The script processed in total `88,605` as can be seen in:
![alt text](../../images/flow_question4.png)

The CLI execution for the deployment was:
`prefect deployment build ./own_tasks_and_files/homework/week_2/etl_web_to_gcs_week2hw_Q4.py:etl_web_to_gcs -n "ETL_HW_Q4" -sb github/dtc-dez-github -o ./own_tasks_and_files/homework/week_2/web_to_gcs_github-deployment.yaml -a`

## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`


## Answer 5. Email or Slack notifications

After creating an E-Mail block with an e-mail, an automation is created whith the trigger being a flow run state and on crashes, failures and time outs an action will be executed in form of sending a notification to the E-Mail block.

The CLI execution for the deployment was: 
`prefect deployment build ./etl_web_to_gcs_week2hw_Q5.py:etl_web_to_gcs -n "ETL_HW_Q5" -a`


The script processed in total `514,392` rows and the sent E-Mail after a deliberated change in code such that it crashes is:
![alt text](../../images/flow_question5.png)

## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8
- 10

## Answer 6. Secrets

The secret block shows `8` asterix even though the password itself has 10 digits.
![alt text](../../images/flow_question6.png)

## Submitting the solutions

* Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 6 February (Monday), 22:00 CET


## Solution

We will publish the solution here
