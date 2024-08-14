var express = require('express');
const { Kafka } = require('kafkajs')
var router = express.Router();

const kafka = new Kafka({
  clientId: 'kafka-retry-app',
  brokers: ['localhost:9092'],
})

async function startConsumer() {
  const consumer = kafka.consumer({ groupId: 'retry5minConsumerGroup' })

  await consumer.connect()
  await consumer.subscribe({ topic: 'retry5min', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      var actualMessage = JSON.parse(message.value.toString());

      let pendingTime = actualMessage.retryTime - Date.now();
      if (pendingTime > 0) {
        consumer.pause([{ topic: "retry5min" }])
        setTimeout(() => {
          console.log(new Date().toString(), "Post resume", actualMessage.message, new Date(actualMessage.retryTime).toString())
          consumer.resume([{ topic: "retry5min" }])
        }, pendingTime)
      } else {
        console.log(new Date().toString(), "Regular", actualMessage.message, new Date(actualMessage.retryTime).toString())

      }
    },
  })
}

startConsumer();


/* GET users listing. */
router.get('/', function (req, res, next) {
  res.send('respond with a resource');
});

router.get('/produceDummyData', async function (req, res, next) {
  const producer = kafka.producer()
  await producer.connect()

  let retryTime = Date.now() + (1000 * 60);

  for (let index = 0; index < 5; index++) {
    await producer.send({
      topic: 'retry5min',
      messages: [
        {
          value: JSON.stringify({
            message: 'Dummy Payload '+index, retryTime: retryTime
          })
        },
      ],
    })
  }

  await producer.disconnect()
  res.send('Dummy message produced');
})

module.exports = router;
