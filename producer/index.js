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


function generateUuid() {
  return Math.random().toString() +
         Math.random().toString() +
         Math.random().toString();
}

// REQ: These names should match the ones used by the Consumer.
const SIMPLE_QUEUE = 'test_queue';
const RPC_QUEUE = 'rpc_queue';

(async () => {
  const connection = await getRabbitConnection();
  const channel = await connection.createChannel();

  await channel.assertQueue(SIMPLE_QUEUE, { durable: false });
  await channel.assertQueue(RPC_QUEUE, { durable: false });

  // Simple messages.
  setInterval(() => {
    const msg = `Simple message at ${new Date().toISOString()}`;

    channel.sendToQueue(SIMPLE_QUEUE, Buffer.from(msg));
    console.log(" [x] Msg sent:", msg);
  }, 4_000);

  // RPC calls.
  const correlationId = generateUuid();
  const replyQueue = await channel.assertQueue('', { exclusive: true });

  channel.consume(replyQueue.queue,
    function(msg) {
      if (msg.properties.correlationId == correlationId) {
        console.log(' [.] RPC Answer:', msg.content.toString());
      }
    },
    { noAck: true }
  );

  setInterval(() => {
    const msg = `${Math.round(Math.random()*10)},${Math.round(Math.random()*10)}`;
    channel.sendToQueue(RPC_QUEUE, Buffer.from(msg), { replyTo: replyQueue.queue, correlationId: correlationId });

    console.log(" [.] RPC Request:", msg, `at ${new Date().toISOString()}`);
  }, 4_000);
})();
