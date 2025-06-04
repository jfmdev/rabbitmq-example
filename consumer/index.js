const amqp = require('amqplib');

(async () => {
  const connection = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await connection.createChannel();
  const queue = 'test_queue';

  await channel.assertQueue(queue, { durable: false });

  console.log(" [*] Waiting for messages...");
  channel.consume(queue, msg => {
    if (msg !== null) {
      console.log(" [x] Received:", msg.content.toString());
      channel.ack(msg);
    }
  });
})();