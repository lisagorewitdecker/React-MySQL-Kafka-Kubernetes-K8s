const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'audit-consumer',
    brokers: [process.env.KAFKA_BROKER || 'kafka-service:9092']
});

const consumer = kafka.consumer({ groupId: 'audit-loggers' });

async function run() {
    await consumer.connect();
    console.log('Consumer connected to Kafka');
    await consumer.subscribe({ topic: 'item-events', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            let event;
            try {
                if (message.value == null) {
                    throw new Error('Message has no value');
                }
                event = JSON.parse(message.value.toString());
            } catch (error) {
                console.warn(`[INVALID EVENT] Discarding message from ${topic}[${partition}]: ${error.message}`);
                return;
            }

            if (
                !event ||
                typeof event !== 'object' ||
                Array.isArray(event) ||
                !event.payload ||
                typeof event.payload !== 'object' ||
                Array.isArray(event.payload) ||
                event.payload.id == null
            ) {
                console.warn(`[INVALID EVENT] Discarding message from ${topic}[${partition}]: invalid event payload`);
                return;
            }

            console.log(`[EVENT RECEIVED] Type: ${event.eventType} | ID: ${event.payload.id} | At: ${event.timestamp}`);
            // Perform background work: audit storage, cache busting, analytics
        }
    });
}

run().catch(async (error) => {
    console.error(error);
    try {
        await consumer.disconnect();
    } finally {
        process.exit(1);
    }
});