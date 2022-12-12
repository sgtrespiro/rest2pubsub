const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { nanoid, customAlphabet} = require('nanoid'); //https://github.com/ai/nanoid#comparison-with-uuid
// const workerize = require('node-inline-worker');

const { PubSub } = require("@google-cloud/pubsub");

const pubsub = new PubSub ({
   projectId: 'respiro-playground',
   keyFilename: 'keys/respiro-playground-2f507787150f.json'
});

const topicNameResponse = process.env.backendTopic || 'bff-response';
const subscriptionNameBase = process.env.responseSubBase || 'response';
const subscriptionName = subscriptionNameBase + '-' + (process.env.myPodId || nanoid(10)); // Use pod metadata injection https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/
console.log(`Topic ${topicNameResponse}, Subscription ${subscriptionName}`);
async function subscr(subscriptionName) {
    let subscription = pubsub.subscription(subscriptionName);

    const [subscrExists] = await subscription.exists();
    if (!subscrExists) {
        console.log(`Creating unique FANOUT subscription ${subscriptionName}`)
        const [subs] = await pubsub.topic(topicNameResponse).createSubscription(subscriptionName);
        subscription = subs;
    }
    if(!subscription.isOpen) {
        subscription.open();
    }
    return subscription;
}

async function subscribeForResponse(subscription, fn = null) {
    return new Promise((resolve, reject) => {
        let onMessage = null;
        let onError = null;
        let onDebug = null;
        let onClose = null;

        const unsubscribe = function () {
            if(onMessage) subscription.removeListener('message', onMessage);
            if(onError) subscription.removeListener('message', onError);
            if(onDebug) subscription.removeListener('message', onDebug);
            if(onClose) subscription.removeListener('message', onClose);
        }
            // Register an error handler.
        onError = (err) => {
            console.error(err);
            if (fn) fn(null, err);
            reject(err);

            unsubscribe();
        };

        // Register a debug handler, to catch non-fatal errors.
        onDebug = (err) => {
            console.debug(err);
        };

        // Register a close handler in case the subscriber closes unexpectedly
        onClose = () => {
            if (fn) fn(null, null);
            reject("Closed unexpectedly");
        };

        // Register a listener for `message` events.
        onMessage = (message) => {
            // Called every time a message is received.

            // message.id = ID of the message.
            // message.ackId = ID used to acknowledge the message receival.
            // message.data = Contents of the message.
            // message.attributes = Attributes of the message.
            // message.publishTime = Date when Pub/Sub received the message.

            // Ack the message:
            // message.ack();

            // This doesn't ack the message, but allows more messages to be retrieved
            // if your limit was hit or if you don't want to ack the message.
            // message.nack();
            const json = JSON.parse(message.data.toString());
            if (fn) fn(json, null);
            resolve(json);
            // Remove the listener from receiving `message` events.
            message.ack();

            unsubscribe();
        }

        subscription.on('error', onError);
        subscription.on('debug', onDebug);
        subscription.on('close', onClose);
        subscription.on('message', onMessage);
    });
}

// Cleanup on exit - delete subscription
process.stdin.resume(); // so the program will not close instantly

let waitBeforeExit = true;
async function deleteSubscriptionBeforeExit(subscription) {
    console.log(`Deleting subscription ${subscriptionName}`);
    await subscription.delete();
    // Deletes the subscription
    console.log(`Subscription ${subscriptionName} deleted.`);
    waitBeforeExit = false;
}

function msleep(n) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, n);
}
function sleep(n) {
    msleep(n*1000);
}

async function exitHandler(options, exitCode) {
    console.warn(`Handling exit: options = ${JSON.stringify(options)}, exitCode = ${exitCode}`);
    console.trace();
    if (options.cleanup) {
        const subscription = pubsub.subscription(subscriptionName);
        try {
            console.log(`${false?"Existing":"Ghost"} subscription ` + subscriptionName);
            const resExists = await subscription.exists();
            console.log(`${true?"Existing":"Ghost"} subscription ` + subscriptionName);
            const [subscrExists] = resExists;
            console.log(`${subscrExists?"Existing":"Ghost"} subscription ` + subscriptionName);
            if (subscrExists) {
                console.log('Cleaning subscription ' + subscriptionName);
                await deleteSubscriptionBeforeExit(subscription); // Is async, so need to wait for completion            
                const start = new Date().getTime();
                while(waitBeforeExit && new Date().getTime() < (start + 2000))
                    msleep(10); // Busy wait until the subscription is deleted - can't async await since the exitHandler can't be async
            }
        } catch(x) {console.log(x)}
    }
    if (exitCode || exitCode === 0) console.log(`Exiting with code ${exitCode}`);
    if (options.exit) process.exit(1);
}

//do something when app is closing
process.on('exit', exitHandler.bind(null,{cleanup:true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {exit:true}));

// catches "kill pid" (for example: nodemon restart)
process.on('SIGUSR1', exitHandler.bind(null, {exit:true}));
process.on('SIGUSR2', exitHandler.bind(null, {exit:true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {exit:true}));

let records = [];

const app = express();

app.use(cors());

// Configuring body parser middleware
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// The name of the topic to which you want to publish messages
const topicNameRequest = 'MY_PUBSUB_TOPIC';

// Accept all incoming HTTP requests
app.all('*', async (req, res) => {
  // Create an object that contains the request body, headers, and URL
  const requestData = {
    body: req.rawBody,
    headers: req.headers,
    url: req.url,
  };

  // Get a reference to the PubSub topic
  const topicRequest = pubsub.topic(topicNameRequest);

  // Publish the request data to the PubSub topic
  topicRequest
    .publishMessage(requestData)
    .then(async () => {
      console.log('Successfully published request to PubSub topic');
      // Wait for response
      const json = await subscribeForResponse(await subscr(subscriptionName));
      res
          .status(200)
          .send(JSON.stringify(json))
          .end();
    })
    .catch(err => {
      // Send an error response to the client
      res.status(500).send(`Error publishing to PubSub topic: ${err.message}`);
    });
});


// app.get('/', async (req, res) => {
//     const json = await subscribeForResponse(await subscr(subscriptionName));
//     res
//         .status(200)
//         .send(JSON.stringify(json))
//         .end();

//     // workerize(function () {
//     //     for(let i = 0; i < 10000; i++)
//     //         console.log(`worked a bit ${i}`);
//     // });            
// });
 
/*
app.post('/record', (req, res) => {
    const record = req.body;

    // Output the record to the console for debugging
    console.log(record);
    records.push(record);

    res.send('Record is added to the database');
});

app.get('/records', (req, res) => {
    res.json(records);
});
*/

// Start the server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`App listening on port ${PORT}`);
  console.log('Press Ctrl+C to quit.');
});


