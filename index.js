// index.js - Lambda function
const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    try {
        for (const record of event.Records) {
            const body = JSON.parse(record.body)
            const message = JSON.parse(body.Message);

            //Create ISO timestamp for consistent sorting
            const currentTimestamp = new Date().toISOString();
            // Prepare item for DynamoDB with composite key

            const item = {
                issueId: message.issueKey,
                timestamp: currentTimestamp,  // This is now part of the composite key
                summary: message.summary,
                priority: message.priority,
                description: message.description,
                creator: message.creator,
                created: message.created,
                lastUpdated: currentTimestamp
            };

            await dynamoDB.put({
                TableName: process.env.DYNAMODB_TABLE,
                Item: item
            }).promise();

            console.log(`Successfully saved issue ${message.issueKey} at timestamp ${currentTimestamp} to DynamoDB`);
        }

        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Successfully processed messages' })
        };
    } catch (error) {
        console.error('Error processing message:', error);
        throw error;
    }
}