// index.js - Lambda function
const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    try {
        for (const record of event.Records) {
            // Single parse for SQS message
            const message = JSON.parse(record.body);

            console.log('Parsed message:', message); // For debugging

            // Extract issue key from the content field
            // Assuming content format: "...Key: TEST-4\nSummary: test t6..."
            const contentLines = message.content.split('\n');
            const issueKey = contentLines.find(line => line.startsWith('Key:'))?.split(':')[1]?.trim();
            const summary = contentLines.find(line => line.startsWith('Summary:'))?.split(':')[1]?.trim();
            const priority = contentLines.find(line => line.startsWith('Priority:'))?.split(':')[1]?.trim();
            const creator = contentLines.find(line => line.startsWith('Creator:'))?.split(':')[1]?.trim();

            const currentTimestamp = new Date().toISOString();

            const item = {
                issueId: issueKey,
                timestamp: currentTimestamp,
                title: message.title,
                summary: summary,
                priority: priority,
                creator: creator,
                content: message.content,
                lastUpdated: currentTimestamp
            };

            // Log the item before saving
            console.log('DynamoDB item to be saved:', item);
            console.log('DynamoDB item to be savedTestttttttt:', item);
            await dynamoDB.put({
                TableName: process.env.DYNAMODB_TABLE,
                Item: item
            }).promise();

            console.log(`Successfully saved issue ${issueKey} to DynamoDB`);
        }

        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Successfully processed messages' })
        };
    } catch (error) {
        console.error('Error processing message:', {
            error: error.message,
            stack: error.stack,
            event: JSON.stringify(event, null, 2)
        });
        throw error;
    }
};