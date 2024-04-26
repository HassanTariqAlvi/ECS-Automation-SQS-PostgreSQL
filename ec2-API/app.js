const express = require('express');
const AWS = require('aws-sdk');
const bodyParser = require('body-parser');
const { Client } = require('pg');
require('dotenv').config();
const clusterName = 'Ecs-Scrape'; // Your ECS cluster name
const taskDefinition = 'ecs-scrapping-td'; // Your ECS task definition


// Create the Express app instance
const app = express();

// Middleware to parse JSON bodies
app.use(bodyParser.json());

// Configure the AWS region and credentials
AWS.config.update({
  region: 'eu-north-1'
  // credentials will be picked up from the environment or AWS configuration
});

// PostgreSQL client configuration
const dbClient = new Client({
  user: 'adminn',
  host: 'ecs-task-db.cdmumsw8mb3d.eu-north-1.rds.amazonaws.com',
  database: 'ecs-task-db',
  password: process.env.DB_PASSWORD, // Use environment variable to store password
  port: 5432,
  ssl: {
    rejectUnauthorized: false // This is not recommended for production
  }
});

// Connect to the PostgreSQL database
dbClient.connect();

// Create an SQS service object
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const queueUrl = "https://sqs.eu-north-1.amazonaws.com/533267065353/ecs-task.fifo";

// POST endpoint to send a message to the SQS queue
app.post('/send', (req, res) => {
  const url = req.body.url;

  if (!url) {
    return res.status(400).send({ message: 'URL is required' });
  }

  // Generate the deduplication ID based on URL
  const deduplicationId = require('crypto').createHash('md5').update(url).digest('hex');

  const params = {
    MessageBody: url,
    QueueUrl: queueUrl,
    MessageGroupId: 'Group1', // Required for FIFO queues
    MessageDeduplicationId: deduplicationId // Required if content-based deduplication is not enabled
  };

  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.log("Error", err);
      res.status(500).send({ error: 'Could not send message to the queue', details: err });
    } else {
      console.log("Success", data.MessageId);
      res.send({ message: 'URL sent successfully!', messageId: data.MessageId });
    }
  });
});

// GET endpoint to retrieve messages from the ecstask table
app.get('/result', async (req, res) => {
  try {
    const result = await dbClient.query('SELECT * FROM ecstask;');
    res.status(200).json(result.rows);
  } catch (error) {
    console.error('Database query error', error);
    res.status(500).send({ error: 'Failed to retrieve messages', details: error });
  }
});

const ecs = new AWS.ECS();
app.get('/queue-count', async (req, res) => {
  const params = {
    QueueUrl: queueUrl,
    AttributeNames: ['ApproximateNumberOfMessages']
  };

  try {
    const data = await sqs.getQueueAttributes(params).promise();
    const messageCount = parseInt(data.Attributes.ApproximateNumberOfMessages, 10);
    res.status(200).send({ count: messageCount });
  } catch (err) {
    console.error('Error retrieving message count from SQS', err);
    res.status(500).send({ error: 'Failed to retrieve message count from SQS', details: err });
  }
});

// Endpoint to retrieve the number of tasks running in the ECS cluster
app.get('/ecs-tasks-count', async (req, res) => {
  const clusterName = 'Ecs-Scrape'; // Replace with your actual ECS cluster name

  try {
    // Listing all tasks in the specified cluster
    const params = {
      cluster: clusterName
    };
    const data = await ecs.listTasks(params).promise();
    const taskCount = data.taskArns.length; // The number of tasks running in the cluster

    res.status(200).send({ taskCount: taskCount });
  } catch (err) {
    console.error('Error retrieving tasks from ECS', err);
    res.status(500).send({ error: 'Failed to retrieve tasks from ECS', details: err });
  }
});


// Function to monitor the SQS queue and launch ECS tasks
async function monitorQueueAndLaunchECSTasks() {
  try {
    const sqsAttributes = await sqs.getQueueAttributes({
      QueueUrl: queueUrl,
      AttributeNames: ['ApproximateNumberOfMessages']
    }).promise();
    const messageCount = parseInt(sqsAttributes.Attributes.ApproximateNumberOfMessages, 10);

    const ecsTasks = await ecs.listTasks({ cluster: clusterName }).promise();
    const runningTasksCount = ecsTasks.taskArns.length;
    const tasksToLaunch = messageCount - runningTasksCount;

    for (let i = 0; i < tasksToLaunch; i++) {
      await ecs.runTask({
        cluster: clusterName,
        launchType: 'FARGATE',
        taskDefinition: taskDefinition,
        count: 1,
        networkConfiguration: {
          awsvpcConfiguration: {
            subnets: ['subnet-0be8ec55c55a57265', 'subnet-042d6a3c5c47f7547'], // Replace with your subnet IDs
            assignPublicIp: 'ENABLED'
          }
        }
      }).promise();
    }
    console.log(`Launched ${tasksToLaunch} tasks.`);
  } catch (err) {
    console.error('Error monitoring queue and launching ECS tasks:', err);
  } finally {
    setTimeout(monitorQueueAndLaunchECSTasks, 1000); // Set the function to run again after 60 seconds
  }
}

// Start the server
const port = 3000;
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
  monitorQueueAndLaunchECSTasks(); // Start the monitoring function
});