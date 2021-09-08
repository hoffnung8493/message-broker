import EventEmitter from "events";
const emitter = new EventEmitter();
import { v4 as uuid } from "uuid";
import { readJSON, writeJSON } from "./fileSystem";

export const EventId = uuid;

export enum Domain {
  INVENTORY = "INVENTORY",
  ORDER = "ORDER",
  PAYMENT = "PAYMENT",
  DELIVERY = "DELIVERY",
  NOTIFICATION = "NOTIFICATION",
}

export enum Topic {
  ORDER_CREATED = "ORDER_CREATED",
  PAYMENT_SUCCESS = "PAYMENT_SUCCESS",
  PRODUCTS_READY = "PRODUCTS_READY",
  ORDER_SHIPPING_READY = "ORDER_SHIPPING_READY",
  INVOICE_READY = "INVOICE_READY",
  PAY_SUCCESS_MESSAGE_DELIVERED = "PAY_SUCCESS_MESSAGE_DELIVERED",
}

/*
events-${topic}.json
stores all the events
*/
interface EventLog {
  topic: Topic;
  events: {
    parentId: string;
    eventId: string;
    content: any;
  }[];
}

/*
queue-${domain}-${topic}.json
stores all queued events
*/

interface DomainTopicQueue {
  subscriber: Domain;
  topic: Topic;
  processedEvents: {
    eventId: string;
    tries: number;
    processedAt: Date;
  }[];
  processingEvents: {
    eventId: string;
    content: string;
    tries: number;
  }[];
}

/*
subscription-error.json
stores all the errors caused by the eventHandler in subscriptions
*/
interface DomainTopicErrors {
  subscriber: Domain;
  topic: Topic;
  eventId: string;
  error: {
    message: string;
    code: string;
    trace: string;
  };
}

/*
dead-letter-queue.json
stores all the events that failed to be processed by a subscriber for more than 5 times.
*/
interface DeadLetterQueue {
  eventId: string;
  topic: Topic;
  subscriber: Domain;
}

/*
SUBSCRIBER Initialization
-(done) check if corresponding 'DomainTopicQueue' exists
-(done) if the file does not exist, retrieve all events from 'EventLog'
-(done) store the events in 'DomainTopicQueue' and for all events run 'executeEventHandler'
- NOTE: it might be necessary to limit the number of max concurrent event handling
-(done) setup eventListener
*/

/*
PUBLISHER
-(done) creates 'eventId'
-(done) persists event in 'EventLog'
-(done) if corresponding 'EventLog' file doesnt exist, it creates the file.
-(done) persists the new 'eventId' to all 'DomainTopicQueue.processingEvents' with same 'topic'
-(done) emit(`event-${topic}`, content, eventId)
*/

/*
SUBSCRIBER
- (done) emit.on(`event-${topic}, (content, eventId) => executeEventHandler(eventId, content))
- (done) 'eventHandler' is the function provided by the developer that processes the event
- (done) 'executeEventHandler' is a wrapper to 'eventHandler'.
- (done) Just before 'eventHandler' is executed internally, 'tries' is incremented
- (done) For every newly emitted event execute 'eventHandler'
- if event takes longer than 5 seconds to process retry
- if 'eventHandler' executes successfully(returns)
    - (done) the event is acknowledged and the corresponding event in 'processingEvents' is moved to "processedEvents"
    - setTimeout is removed
- if 'eventHandler' fails OR eventHandler takes longer than 5 seconds
    - store the error in 'ErrorLog' and retry again. This is very useful for debugging
    - (done) Like the first retry, every retry get another 5 seconds
    - After 5 times of retry, send the event to 'DeadLetterQueue'
*/

/*
TODO
- for the event 'content' replace 'any' type with a generic type.
- need to lock files when writing to it, in order to prevent overwriting
- After bugs are fixed, the events in 'DeadLetterQueue' must be to republished. But only to the subscribers that caused errors. NOT ALL SUBSCRIBERS
- prevent from duplicate processing when subscription initialization is duplicated
- switch from JSON files to memory and a single commit log. 
  Commit log is append only, so it can be written much faster.
  - later on we can add a converter that converts the commit logs into JSON files, which can be stored in S3 bucket.

-(done) currently after event is acknowledged(processed), the number of tries is lost(processingEvents pop). This data could be usefull for performance analysis

IDEAS
- GUI. A dashboard where it shows:
  - all the events, 
  - how one type of event triggers other events, causing event chain
  - analytics on each event type
  - show connected subscribers
  - show cluster of Node.js processes(each process will have its own message broker)
  - show errors and most importantly dead letter queue, which is basically a queue filled with events
    that failed to be processed by certain subscribers. 
    Developers can look at this queue,see the error logs(message, error trace), fix the bug
    and finally republish the failed events. This allows eventual consistency!
    This is a great UX boost. In most cases users will not be aware of these bugs. 
    These bugs will be fixed behind the scenes without the users knowing,
    whereas in conventional request/response scenario, users would receive errors.
  - Each event should be small and the name of event should be enough to understand what it does.
    But if developers want to add more explanation, we could support documentation features.
    This is also very useful in terms of Domain Driven Design.
    Not only new developers but domain experts could easily understand the system
  - Naturally we could support comment/feedback feature for each topic
  - Support schema registry. Possibly with mock data, subscribers can start working on their code,
    without waiting for the publisher to finish their code.
  - Event sourcing platform

- Keep the log files small for fast IO
  Log files can get big really fast. To keep the message broker as light as possible,
  log files should be kept very small. When the log file reaches a certain size, we could make a new file,
  where the new file can point to the old file(like linked list or bucket pattern)
  Most cases, events will be processed by all the subscribers within seconds.
  But old messages are still necessary for two scenarios:
  - Addition of a new subscriber that requires all the messages. 
    In this case instead of republishing the old messages, the message broker can simply grab all the messages
    and give it to the new subscriber directly
  - Data processing & machine learning

- Tiered Storage(S3)
  This feature is for really big apps, where you generate gigabytes of logs. 
  In this case it might be a good idea to move the old logs to a cheap file storage such as S3

- Support for 3rd party central message brokers such as Kafka, NATS Jetstream, Redis Stream etc.
  Later when we transition to micro service architecture, where domains are no longer in a single Node.js process,
  we will need a centralized real time message broker that can communicate with all the processes.
  We could support plugins for different existing message brokers.
  Just like ORMs supporting multiple databases.
  - Also we could give an option to use both local file system message broker and central message broker. 
    In this case, messages would be published to both local and central message broker.
    This allows efficient event driven communication within each process, 
    while allowing other processes to subscribe these events as well.
    Also even if the central message broker dies, each Node.js process can still work normally.
    The events not delivered to the central message broker can be resent as soon as the central message broker comes back live.
    
    In most cases event driven monolith will suffice, one of the biggest use case would be when we need Python servers dedicated for data processing & machine learning
    But even for these cases, if its not real time data processing, central message broker might not necessary
    All messages will be available in the database or external file storage(S3).
    So Python servers could simply run batches to fetch data from these storage.
*/

const subscribers: {
  subscriber: Domain;
  topics: Topic[];
}[] = [];

const topics: {
  topic: Topic;
  subscribers: Domain[];
}[] = [];

export const subscriberInitializer = (subscriber: Domain) => {
  let subscriberObj = subscribers.find((v) => v.subscriber === subscriber);
  if (subscriberObj)
    throw new Error(`${subscriber} subscriber is being initialized twice.`);

  subscriberObj = { subscriber, topics: [] };
  subscribers.push(subscriberObj);

  return async (
    topic: Topic,
    eventHandler: (parentId: string, arg: any) => Promise<any>
  ) => {
    const topicIsSubscribed = subscriberObj!.topics.find((v) => v === topic);
    if (topicIsSubscribed)
      throw new Error(
        `${topic} topic is being subscribed twice by the same ${subscriber} subscriber`
      );
    subscriberObj!.topics.push(topic);

    let topicObj = topics.find((t) => t.topic === topic);
    if (!topicObj) {
      topicObj = { topic, subscribers: [subscriber] };
      topics.push(topicObj);
    } else {
      topicObj.subscribers.push(subscriber);
    }

    const fileName = `queue-${subscriber}-${topic}.json`;
    let domainTopicQueue = await readJSON<DomainTopicQueue>(fileName);
    if (!domainTopicQueue) {
      const topicLogs = await readJSON<EventLog>(`events-${topic}.json`);
      domainTopicQueue = await writeJSON<DomainTopicQueue>(fileName, {
        subscriber,
        topic,
        processedEvents: [],
        processingEvents: topicLogs
          ? topicLogs.events.map(({ eventId, content }) => ({
              eventId,
              content,
              tries: 0,
            }))
          : [],
      });
    }
    await Promise.all(
      domainTopicQueue.processingEvents.map(({ eventId, content, tries }) =>
        executeEventHandler(
          eventHandler,
          subscriber,
          topic,
          content,
          tries,
          eventId
        )
      )
    );

    emitter.on(`domain-event-${topic}`, (content: any, eventId: string) => {
      executeEventHandler(eventHandler, subscriber, topic, content, 0, eventId);
    });
  };
};

//this function probably should not be called multiple times for same subscriber & topic & parentId
const executeEventHandler = async (
  eventHandler: (parentId: string, arg: any) => Promise<any>,
  subscriber: Domain,
  topic: Topic,
  content: any,
  tries: number,
  parentId: string
) => {
  let acked = false;
  let triesCount = tries;
  while (!acked && triesCount < 5) {
    triesCount++;
    await tryProcessing(
      subscriber,
      topic,
      parentId,
      content,
      eventHandler,
      triesCount !== tries
    )
      .then(() => (acked = true))
      .catch((err) => {});
  }
};

const tryProcessing = (
  subscriber: Domain,
  topic: Topic,
  parentId: string,
  content: any,
  eventHandler: (parentId: string, arg: any) => Promise<any>,
  isRetry: boolean
) =>
  new Promise(async (resolve, reject) => {
    if (isRetry) {
      setTimeout(async () => {
        try {
          //maybe the two async functions can be called together with Promise.all
          await incrementTries(subscriber, topic, parentId);
          await eventHandler(parentId, content);
          await eventAcknowledged(subscriber, topic, parentId);
          resolve(null);
        } catch (err) {
          reject(err);
        }
      }, 5000);
    } else {
      try {
        await incrementTries(subscriber, topic, parentId);
        await eventHandler(parentId, content);
        await eventAcknowledged(subscriber, topic, parentId);
        resolve(null);
      } catch (err) {
        reject(err);
      }
    }
  });

const eventAcknowledged = async (
  subscriber: Domain,
  topic: Topic,
  parentId: string
) => {
  const fileName = `queue-${subscriber}-${topic}.json`;
  const domainTopicQueue = await readJSON<DomainTopicQueue>(fileName);
  if (!domainTopicQueue)
    throw new Error(`Someting went wrong! - ${fileName} does not exist!`);
  const index = domainTopicQueue.processingEvents.findIndex(
    (v) => v.eventId === parentId
  );
  if (index === -1)
    throw new Error(
      `Something went wrong! - processed event cannot be found in processingEvents - ${subscriber}, ${topic}, ${parentId}`
    );
  const processedEvent = domainTopicQueue.processingEvents.splice(index, 1);
  domainTopicQueue.processedEvents.push({
    eventId: parentId,
    tries: processedEvent[0].tries,
    processedAt: new Date(),
  });
  domainTopicQueue.processingEvents = domainTopicQueue.processingEvents.filter(
    (v) => v.eventId !== parentId
  );
  writeJSON<DomainTopicQueue>(fileName, domainTopicQueue);
};

const incrementTries = async (
  subscriber: Domain,
  topic: Topic,
  eventId: string
) => {
  const fileName = `queue-${subscriber}-${topic}.json`;
  const topicLogs = await readJSON<DomainTopicQueue>(fileName);
  if (!topicLogs)
    throw new Error(`Something went wrong! ${fileName} does not exist`);
  topicLogs.processingEvents.find((v) => v.eventId === eventId)!.tries++;
  await writeJSON<DomainTopicQueue>(fileName, topicLogs);
};

export const publish = async (parentId: string, topic: Topic, content: any) => {
  const { eventId } = await persistEvent(topic, content, parentId);
  await persistEventsInSubscriberQueues(eventId, topic, content);
  emitter.emit(`domain-event-${topic}`, content, eventId);
  console.log(`### Event Published! [${topic}] - ${eventId}`);
};

const persistEventsInSubscriberQueues = async (
  eventId: string,
  topic: Topic,
  content: any
) => {
  const topicObj = topics.find((v) => v.topic === topic);
  if (!topicObj) return;
  topicObj.subscribers.map(async (subscriber) => {
    const fileName = `queue-${subscriber}-${topic}.json`;
    const logs = await readJSON<DomainTopicQueue>(fileName);

    if (!logs) return;
    logs.processingEvents.push({ eventId, content, tries: 0 });
    await writeJSON<DomainTopicQueue>(fileName, logs);
  });
};

const persistEvent = async (
  topic: Topic,
  content: any,
  parentId: string
): Promise<{ eventId: string }> =>
  new Promise(async (resolve, reject) => {
    const eventId = uuid();
    const fileName = `events-${topic}.json`;
    try {
      let eventLogs = await readJSON<EventLog>(fileName);
      if (!eventLogs) {
        eventLogs = {
          topic,
          events: [],
        };
      }
      eventLogs.events.push({ parentId, eventId, content });
      await writeJSON<EventLog>(fileName, eventLogs);
      resolve({ eventId });
    } catch (err) {
      reject(err);
    }
  });

/*
TODO
- if subscriber is initialized after a publisher throw error
- use raw text instead of JSON. Parsing JSON can be expensive
- need to create locks while a file is beaing read and updated
- Initialize subscriber even if no topic has been subscribed to
*/
