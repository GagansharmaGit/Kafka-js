import { Kafka } from "kafkajs"

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'region_Asia' })

const run = async () => {
  // Producing
  await producer.connect()
  await producer.send({
    topic: 'Consumer_meta_data',
    messages: [
      { value: '{"user_id": "User1", username:"Gagan Sharma"}' },
    ],
  })

  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'Consumer_meta_data', fromBeginning: true })

  await consumer.run({
    // @ts-ignore
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
}

run().catch(console.error)