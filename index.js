const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { nanoid, customAlphabet} = require('nanoid'); //https://github.com/ai/nanoid#comparison-with-uuid
const workerize = require('node-inline-worker');

const { PubSub } = require("@google-cloud/pubsub");

const pubsub = new PubSub ({
   projectId: 'your-project-id',
   keyFilename: '/path/to/keyfile.json'
});

const topicName = process.env.backendTopic || 'backend-service1';
const subscriptionNameBase = process.env.responseSubBase || 'response';
const subscriptionName = subscriptionNameBase + '-' + (process.env.myPodId || nanoid(10)); // Use pod metadata injection https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/
console.log(`Topic ${topicName}, Subscription ${subscriptionName}`);
async function subscr() {
    let subscription = pubsub.subscription(subscriptionName);

    if (!subscription) {
        const [subs] = await pubsub.topic(topicName).createSubscription(subscriptionName);
        subscription = subs;
    }
    if(!subscription.isOpen) {
        subscription.open();
    }
    return subscription;
}

async function subscribeForResponse(subscription, fn = null) {
    return new Promise((resolve, reject) => {
            // Register an error handler.
        subscription.on('error', (err) => { 
            console.error(err); 
            if(fn) fn(null, err);
            reject(err);
            // Remove the listener from receiving `message` events.
            subscription.removeListener('message', onMessage);
        });

        // Register a debug handler, to catch non-fatal errors.
        subscription.on('debug', (err) => { 
            console.debug(err); 
        });

        // Register a close handler in case the subscriber closes unexpectedly
        subscription.on('close', () => {
            if(fn) fn(null, null);
            reject("Closed unexpectedly");
        });

        // Register a listener for `message` events.
        function onMessage(message) {
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
            if(fn) fn(message.data, null);
            resolve(message.data);       
            // Remove the listener from receiving `message` events.
            subscription.removeListener('message', onMessage);
            message.ack();
        }
        subscription.on('message', onMessage);
    });
}

// Cleanup on exit - delete subscription
process.stdin.resume(); // so the program will not close instantly

let waitBeforeExit = true;
function deleteSubscriptionBeforeExit(subscription) {
    console.log(`Deleting subscription ${subscriptionName}`);
    subscription.delete().then(() => {
        // Deletes the subscription
        console.log(`Subscription ${subscriptionName} deleted.`);
        waitBeforeExit = false;
    }).catch(console.log);
}

function msleep(n) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, n);
}
function sleep(n) {
    msleep(n*1000);
}

function exitHandler(options, exitCode) {
    console.warn(`Handling exit: options = ${JSON.stringify(options)}, exitCode = ${exitCode}`);
    if (options.cleanup) {
        console.log('Cleaning subscription');
        const subscription = pubsub.subscription(subscriptionName);
        if (subscription) {
            deleteSubscriptionBeforeExit(subscription); // Is async, so need to wait for completion
            // workerize(function () {
            // });            
            
            const start = new Date().getTime();
            while(waitBeforeExit && new Date().getTime() < (start + 2000))
               msleep(10); // Busy wait until the subscription is deleted - can't async await since the exitHandler can't be async
        }
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

app.get('/', async (req, res) => {
    // deleteSubscriptionBeforeExit(pubsub.subscription(subscriptionName)); // Is async, so need to wait for completion
    const subscription = await subscr();
    const data = await subscribeForResponse(subscription);
    res
        .status(200)
        .send('Hello, world!')
        .end();
});
 
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

// Start the server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`App listening on port ${PORT}`);
  console.log('Press Ctrl+C to quit.');
});
