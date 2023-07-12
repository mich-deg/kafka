const { Kafka } = require('kafkajs')


const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['localhost:9092'],
})

const consumer = kafka.consumer({ groupId: 'animal-group' })
const topic = "animals"



const run = async () => {
    // Consuming
    await consumer.connect()
    await consumer.subscribe({ topic })

    await consumer.run({
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