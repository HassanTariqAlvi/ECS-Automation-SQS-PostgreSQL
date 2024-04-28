const AWS = require("aws-sdk");
const express = require("express");
const { Client } = require("pg");
require("dotenv").config();

// Setup AWS Services
AWS.config.update({ region: "eu-north-1" });
const sqs = new AWS.SQS({ apiVersion: "2012-11-05" });
const ecs = new AWS.ECS();
const queueUrl =
  "https://sqs.eu-north-1.amazonaws.com/533267065353/ecs-task.fifo";
const clusterName = "Ecs-Scrape";
const taskDefinition = "ecs-scrapping-td";

// PostgreSQL client configuration
const client = new Client({
  user: "adminn",
  host: "ecs-task-db.cdmumsw8mb3d.eu-north-1.rds.amazonaws.com",
  database: "ecs-task-db",
  password: process.env.DB_PASSWORD,
  port: 5432,
  ssl: { rejectUnauthorized: false },
});
client.connect();

const app = express();
const port = 4000;

async function pollMessages() {
  try {
    const params = {
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 1,
      VisibilityTimeout: 90,
      WaitTimeSeconds: 20
    };
    const data = await sqs.receiveMessage(params).promise();

    if (data.Messages && data.Messages.length > 0) {
      const message = data.Messages[0];
      console.log("Successfully picked up URL:", message.Body);
       await new Promise(resolve => setTimeout(resolve, 5000));

      // Retrieve the current number of messages in the queue
      const sqsAttributes = await sqs.getQueueAttributes({
        QueueUrl: queueUrl,
        AttributeNames: ['ApproximateNumberOfMessages']
      }).promise();
      const messageCount = parseInt(sqsAttributes.Attributes.ApproximateNumberOfMessages, 10);
      console.log(`Current number of messages in queue: ${messageCount}`);

      // Retrieve details on the current number of running tasks
      const ecsTasks = await ecs.listTasks({ cluster: clusterName, desiredStatus: 'RUNNING' }).promise();
      let runningTasksCount = 0;
      let taskDescriptions;
      if (ecsTasks.taskArns.length > 0) {
        taskDescriptions = await ecs.describeTasks({ cluster: clusterName, tasks: ecsTasks.taskArns }).promise();
        runningTasksCount = taskDescriptions.tasks.filter(task => ['RUNNING', 'PROVISIONING', 'PENDING'].includes(task.lastStatus)).length;
      } else {
        console.log("No tasks are currently running, provisioning, or pending.");
      }
      console.log(`Current number of running tasks: ${runningTasksCount}`);

      // Determine the number of tasks to launch, if any
      const activeTasksCount = taskDescriptions ? taskDescriptions.tasks.filter(task => task.lastStatus === 'RUNNING').length : 0;
      console.log(`Current number of active tasks: ${activeTasksCount}`);
      const tasksToLaunch = Math.max(0, messageCount + 1 - activeTasksCount);
      console.log(`Number of additional tasks to launch: ${tasksToLaunch}`);
      for (let i = 0; i < tasksToLaunch; i++) {
        await launchNewTask();
      }

      // Process the message
      const deleteParams = {
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle
      };
      await sqs.deleteMessage(deleteParams).promise();
      console.log("Successfully removed the URL from SQS");

      await client.query('INSERT INTO ecstask(message, url) VALUES($1, $2)', [message.Body, 'Success']);
      console.log("Successfully inserted the success message into the database");
    } else {
      console.log("No messages to process");
    }

    // Manage task exit based on current task count
    const currentTasks = await ecs.listTasks({ cluster: clusterName }).promise();
    const currentActiveCount = currentTasks.taskArns.length;
    if (currentActiveCount <= 1) {
      console.log("Only one or zero active tasks running, keeping this task alive.");
      setTimeout(pollMessages, 3000); // Continue polling without exiting
    } else {
      console.log("Sufficient active tasks running, safe to exit this one.");
      process.exit(0);
    }
  } catch (err) {
    console.error("Error during message processing:", err);
    setTimeout(pollMessages, 3000); // Ensure continuous polling even after an error
  }
}


async function launchNewTask() {
  try {
    const taskCount = 1; // Ensure this is never 0 when intending to launch tasks
    if (taskCount > 0) {
      const result = await ecs
        .runTask({
          cluster: clusterName,
          launchType: "FARGATE",
          taskDefinition: taskDefinition,
          count: taskCount,
          networkConfiguration: {
            awsvpcConfiguration: {
              subnets: ["subnet-0be8ec55c55a57265", "subnet-042d6a3c5c47f7547"],
              assignPublicIp: "ENABLED",
            },
          },
        })
        .promise();
      console.log(
        "ECS task launched:",
        result.tasks.map((task) => task.taskArn)
      );
    } else {
      console.log("No tasks launched due to task count being zero.");
    }
  } catch (err) {
    console.error("Error launching ECS task:", err);
  }
}

pollMessages();
app.listen(port, () => {
  console.log(`Server is running and polling SQS on http://localhost:${port}`);
});
