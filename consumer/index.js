import amqp from 'amqplib';

const CONNECTION_ATTEMPTS = 30;

async function getRabbitConnection(retry = CONNECTION_ATTEMPTS) {
  try{
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    return connection;
  }catch(err) {
    if(retry > 0) {
      await new Promise(resolve => setTimeout(resolve, 2_000));
      return getRabbitConnection(--retry);
    } else {
      throw err
    }
  }
}

// REQ: These names should match the ones used by the Producer.
const SIMPLE_QUEUE = 'test_queue';
const RPC_QUEUE = 'rpc_queue';

(async () => {
  const connection = await getRabbitConnection();
  const channel = await connection.createChannel();

  await channel.assertQueue(SIMPLE_QUEUE, { durable: false });
  await channel.assertQueue(RPC_QUEUE, { durable: false });
  channel.prefetch(1);

  console.log(" [*] Waiting for messages...");

  // Simple messages.
  channel.consume(SIMPLE_QUEUE, msg => {
    if (msg !== null) {
      console.log(" [x] Msg received:", msg.content.toString());
      channel.ack(msg);
    }
  });

  // RPC calls.
  channel.consume(RPC_QUEUE, function reply(msg) {
    const numbers = msg.content.toString().split(',');
    const result = parseInt(numbers[0]) + parseInt(numbers[1]);
    console.log(" [.] RPC received:", numbers);

    channel.sendToQueue(
      msg.properties.replyTo,
      Buffer.from(result.toString()),
      { correlationId: msg.properties.correlationId }
    );

    channel.ack(msg);
  });
})();