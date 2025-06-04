const amqp = require('amqplib');

(async () => {
  const connection = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await connection.createChannel();
  const queue = 'test_queue';

  await channel.assertQueue(queue, { durable: false });

  setInterval(() => {
    const msg = `Message at ${new Date().toISOString()}`;
    channel.sendToQueue(queue, Buffer.from(msg));
    console.log(" [x] Sent:", msg);
  }, 2000);
})();