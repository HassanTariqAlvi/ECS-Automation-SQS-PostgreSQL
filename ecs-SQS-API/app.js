const AWS = require('aws-sdk');
const express = require('express');
const { Client } = require('pg');
require('dotenv').config();

// Setup AWS Services
AWS.config.update({ region: 'eu-north-1' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const ecs = new AWS.ECS();
const queueUrl = "https://sqs.eu-north-1.amazonaws.com/533267065353/ecs-task.fifo";
const clusterName = 'Ecs-Scrape';
const taskDefinition = 'ecs-scrapping-td';

// PostgreSQL client configuration
const client = new Client({
  user: 'adminn',
  host: 'ecs-task-db.cdmumsw8mb3d.eu-north-1.rds.amazonaws.com',
  database: 'ecs-task-db',
  password: process.env.DB_PASSWORD,
  port: 5432,
  ssl: { rejectUnauthorized: false }
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

      // Additional processing logic here...

      await launchNewTask();  // Consider moving this inside conditional checks based on message/task count logic.
      console.log("Additional tasks launched if necessary.");

      await sqs.deleteMessage({
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle
      }).promise();
      console.log("Successfully removed the URL from SQS");

      await client.query('INSERT INTO ecstask(message, url) VALUES($1, $2)', [message.Body, 'Success']);
      console.log("Successfully inserted the success message into the database");
      await new Promise(resolve => setTimeout(resolve, 25000));
      console.log("Continuing after 25-second pause");

    } else {
      console.log("No messages to process");
    }
  } catch (err) {
    console.error("Error during message processing:", err);
  } finally {
    setTimeout(pollMessages, 30000);
  }
}

async function launchNewTask() {
  try {
    await ecs.runTask({
      cluster: clusterName,
      launchType: 'FARGATE',
      taskDefinition: taskDefinition,
      count: 1,
      networkConfiguration: {
        awsvpcConfiguration: {
          subnets: ['subnet-0be8ec55c55a57265', 'subnet-042d6a3c5c47f7547'],
          assignPublicIp: 'ENABLED'
        }
      }
    }).promise();
    console.log("ECS task launched.");
  } catch (err) {
    console.error("Error launching ECS task:", err);
  }
}

pollMessages();
app.listen(port, () => {
  console.log(`Server is running and polling SQS on http://localhost:${port}`);
});
