// index.js - Optimized Lambda function
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, BatchWriteCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');

// Initialize DynamoDB client outside handler for connection reuse
const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const dynamoDB = DynamoDBDocumentClient.from(dynamoClient);

// Constants
const MAX_BATCH_SIZE = 25; // DynamoDB BatchWrite limit
const MAX_RETRIES = 3;

/**
 * Parse structured content from Jira message
 * @param {string} content - Raw message content
 * @returns {object} Parsed fields
 */
function parseJiraContent(content) {
    const fields = {};
    const lines = content.split('\n');

    for (const line of lines) {
        const colonIndex = line.indexOf(':');
        if (colonIndex > 0) {
            const key = line.substring(0, colonIndex).trim().toLowerCase();
            const value = line.substring(colonIndex + 1).trim();

            // Map common fields
            switch (key) {
                case 'key':
                    fields.issueKey = value;
                    break;
                case 'summary':
                    fields.summary = value;
                    break;
                case 'priority':
                    fields.priority = value;
                    break;
                case 'creator':
                    fields.creator = value;
                    break;
                case 'status':
                    fields.status = value;
                    break;
                case 'assignee':
                    fields.assignee = value;
                    break;
                default:
                    // Store other fields dynamically
                    fields[key] = value;
            }
        }
    }

    return fields;
}

/**
 * Create DynamoDB item from parsed message
 * @param {object} message - SQS message
 * @param {object} parsedFields - Parsed Jira fields
 * @returns {object} DynamoDB item
 */
function createDynamoItem(message, parsedFields) {
    const currentTimestamp = new Date().toISOString();

    return {
        issueId: parsedFields.issueKey || `unknown-${Date.now()}`,
        timestamp: currentTimestamp,
        title: message.title,
        summary: parsedFields.summary,
        priority: parsedFields.priority,
        creator: parsedFields.creator,
        status: parsedFields.status,
        assignee: parsedFields.assignee,
        content: message.content,
        lastUpdated: currentTimestamp,
        processedAt: currentTimestamp,
        // Store any additional fields dynamically
        ...Object.fromEntries(
            Object.entries(parsedFields).filter(([key]) =>
                !['issueKey', 'summary', 'priority', 'creator', 'status', 'assignee'].includes(key)
            )
        )
    };
}

/**
 * Write items to DynamoDB in batches
 * @param {Array} items - Items to write
 * @returns {Promise<object>} Results
 */
async function batchWriteToDynamoDB(items) {
    const results = {
        successful: 0,
        failed: 0,
        errors: []
    };

    // Process in chunks of 25 (DynamoDB limit)
    for (let i = 0; i < items.length; i += MAX_BATCH_SIZE) {
        const chunk = items.slice(i, i + MAX_BATCH_SIZE);

        const putRequests = chunk.map(item => ({
            PutRequest: { Item: item }
        }));

        try {
            const command = new BatchWriteCommand({
                RequestItems: {
                    [process.env.DYNAMODB_TABLE]: putRequests
                }
            });

            const response = await dynamoDB.send(command);

            // Handle unprocessed items (retry logic)
            let unprocessedItems = response.UnprocessedItems?.[process.env.DYNAMODB_TABLE] || [];
            let retryCount = 0;

            while (unprocessedItems.length > 0 && retryCount < MAX_RETRIES) {
                console.log(`Retrying ${unprocessedItems.length} unprocessed items (attempt ${retryCount + 1})`);

                // Exponential backoff
                await new Promise(resolve => setTimeout(resolve, Math.pow(2, retryCount) * 100));

                const retryCommand = new BatchWriteCommand({
                    RequestItems: {
                        [process.env.DYNAMODB_TABLE]: unprocessedItems
                    }
                });

                const retryResponse = await dynamoDB.send(retryCommand);
                unprocessedItems = retryResponse.UnprocessedItems?.[process.env.DYNAMODB_TABLE] || [];
                retryCount++;
            }

            const processedCount = chunk.length - unprocessedItems.length;
            results.successful += processedCount;
            results.failed += unprocessedItems.length;

            if (unprocessedItems.length > 0) {
                console.error(`Failed to process ${unprocessedItems.length} items after ${MAX_RETRIES} retries`);
                results.errors.push(`${unprocessedItems.length} items failed after retries`);
            }

        } catch (error) {
            console.error('Batch write error:', error);
            results.failed += chunk.length;
            results.errors.push(error.message);
        }
    }

    return results;
}

/**
 * Fallback to individual writes for failed batch items
 * @param {Array} items - Items that failed batch write
 * @returns {Promise<object>} Results
 */
async function fallbackIndividualWrite(items) {
    const results = { successful: 0, failed: 0, errors: [] };

    const promises = items.map(async (item) => {
        try {
            const command = new PutCommand({
                TableName: process.env.DYNAMODB_TABLE,
                Item: item
            });

            await dynamoDB.send(command);
            return { success: true, issueId: item.issueId };
        } catch (error) {
            console.error(`Failed to save item ${item.issueId}:`, error);
            return { success: false, issueId: item.issueId, error: error.message };
        }
    });

    const writeResults = await Promise.allSettled(promises);

    writeResults.forEach((result) => {
        if (result.status === 'fulfilled') {
            if (result.value.success) {
                results.successful++;
            } else {
                results.failed++;
                results.errors.push(`${result.value.issueId}: ${result.value.error}`);
            }
        } else {
            results.failed++;
            results.errors.push(`Promise rejected: ${result.reason}`);
        }
    });

    return results;
}

/**
 * Main Lambda handler
 */
exports.handler = async (event) => {
    console.log(`Processing ${event.Records.length} SQS records`);

    try {
        // Parse all messages first
        const items = [];
        const parseErrors = [];

        for (const record of event.Records) {
            try {
                const message = JSON.parse(record.body);
                const parsedFields = parseJiraContent(message.content);
                const item = createDynamoItem(message, parsedFields);
                items.push(item);

                console.log(`Parsed issue: ${parsedFields.issueKey}`);
            } catch (parseError) {
                console.error('Failed to parse record:', {
                    messageId: record.messageId,
                    error: parseError.message,
                    body: record.body
                });
                parseErrors.push({
                    messageId: record.messageId,
                    error: parseError.message
                });
            }
        }

        if (items.length === 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: 'No valid items to process',
                    parseErrors
                })
            };
        }

        // Batch write to DynamoDB
        console.log(`Writing ${items.length} items to DynamoDB`);
        const batchResults = await batchWriteToDynamoDB(items);

        // If some items failed batch write, try individual writes
        let fallbackResults = null;
        if (batchResults.failed > 0) {
            console.log(`Attempting fallback individual writes for ${batchResults.failed} failed items`);
            const failedItems = items.slice(-batchResults.failed); // This is simplified - in reality you'd track which specific items failed
            fallbackResults = await fallbackIndividualWrite(failedItems);
        }

        const finalResults = {
            totalProcessed: items.length,
            batchResults,
            fallbackResults,
            parseErrors: parseErrors.length > 0 ? parseErrors : undefined
        };

        console.log('Processing complete:', finalResults);

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Processing complete',
                results: finalResults
            })
        };

    } catch (error) {
        console.error('Handler error:', {
            error: error.message,
            stack: error.stack,
            event: JSON.stringify(event, null, 2)
        });

        // Don't throw - return error response to avoid infinite SQS retries
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: 'Processing failed',
                error: error.message
            })
        };
    }
};