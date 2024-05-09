const AWS = require('aws-sdk');
const express = require('express');
const { Client } = require('pg');
require('dotenv').config();


// PostgreSQL client configuration
const client = new Client({
  user: 'adminn',
  host: 'ecs-task-db.c9skawog8vc9.us-west-2.rds.amazonaws.com',
  database: 'ecs-task-db',
  password: process.env.DB_PASSWORD, // Replace with your actual password
  port: 5432,
  ssl: {
    rejectUnauthorized: false // This is not recommended for production
  }


});

// Connect to the PostgreSQL client
client.connect();

// Configure the AWS region
AWS.config.update({
  region: 'us-west-2'
});

// Create an SQS service object
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const queueUrl = "https://sqs.us-west-2.amazonaws.com/533267065353/ecs-task.fifo"; // Replace with your queue URL

const app = express();
const port = 4000; // The port the server will listen on

// Function to poll messages from the queue
function pollMessages() {
  const params = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 1, // How many messages to retrieve
    VisibilityTimeout: 90, // Messages are hidden during the visibility timeout
    WaitTimeSeconds: 20 // Enable long polling
  };

  sqs.receiveMessage(params, async (err, data) => {
    if (err) {
      console.log("Receive Error", err);
      process.exit(1);
    } else if (data.Messages && data.Messages.length > 0) {
      const message = data.Messages[0];
      console.log("Successfully picked up URL:", message.Body);

      // Delete the message from the queue after picking it up
      const deleteParams = {
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle
      };
      try {
        await sqs.deleteMessage(deleteParams).promise();
        console.log("Successfully removed the URL from SQS");
        const insertRes = await client.query('INSERT INTO ecstask(message, url) VALUES($1, $2)', [message.Body, 'Success']);
        console.log("Successfully inserted the success message into the database");
        process.exit(0); // Exit after successful insertion
      } catch (err) {
        console.log("Error handling message:", err);
        process.exit(1); // Exit with an error code
      }
    } else {
      console.log("No messages to process");
      process.exit(0); // Exit when there are no messages
    }
  });
}

// Start polling messages from SQS on server start
pollMessages();

setInterval(pollMessages, 30000); // Poll every 30 seconds

app.listen(port, () => {
  console.log(`Server is running and polling SQS on http://localhost:${port}`);
});
