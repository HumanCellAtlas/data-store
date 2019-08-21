Asynchronous notification
=========================

In the context of the DSS Data Store, a notification is defined as the invocation of a PUT or POST request against an
HTTP or HTTPS endpoint at the time a particular type of event occurs within the data store. The only event for which
notifications are currently supported is that of a new bundle version beeing indexed. Registering an endpoint to be
notified in an event of a certain type results in the creation of a persistent *subscription* object. The system will
notify the subscribed endpoint every time such an event occurs. The subscription for a *bundle version indexed* event
can be configured to limit notifications to new bundle versions whose metadata matches a particular ElasticSearch query.

A notification is known to be synchronous if it is delivered in the context of the data store thread that handles the
event. It is considered asynchronous if it is delivered by a separate data store thread, enabling the event handling
thread to continue its work without having to deal with outages of the endpoint to be notified. DSS Data Store
notifications are represented by instances of the `Notification` class. The actual queueing and retry logic for
notifications resides in the `Notifier` class. For synchronous notifications, the `Notification.deliver()` method is
invoked directly. For asynchronous notifications, the `Notification` instance is instead passed to the `enqueue()`
method of a `Notifier` instance. That method wraps the `Notification` instance in an SQS message and enqueues it in an
SQS queue.

The `Notifier.run()` method starts the asynchronous notification process. It launches a number of worker threads that
each pull a message from a queue, unwrap the `Notification` instance inside the message and invoke `deliver()` on it.
If `deliver()` fails or times out, the message is put back onto an SQS queue where it waits for another attempt.

The `Notifier.enqueue()` method is typically invoked by the `dss-index` daemon in response to an S3 event about a
bundle that is to be indexed. The `Notifier.run()` method is typically invoked by the `dss-notify` daemon which is
started periodically by a CloudWatch rule.

There are multiple SQS queues, one per delivery attempt. The queues are numbered, starting with 0. All new
notifications are enqueued in the queue with index 0. All notifications that failed their first delivery attempt are
placed in queue 1 and so on. Each queue has at minimum one worker thread assigned to it. Having multiple queues
guarantees the fairness and responsiveness of the design: newly enqueued notifications are handled with the same
priority as older notifications that already had to be retried. The presence of many notifications that have to be
retried will not affect the ability of the system to handle new notifications. It allows the system to keep up with
ongoing new work, while concurrently recovering from prior outages of itself or individual notification endpoints.

Each queue is associated with a fixed maturity delay in seconds. The first queue (queue 0) typically has a maturity
delay of 0. A queue message (and the Notification instance it contains) is considered mature once it spends at least
that amount of time in the queue. The important invariant is that the messages in a queue mature in FIFO order. This
enables the worker thread to put itself to sleep until the message at the beginning of the queue matures. This is
acceptable because the messages behind it are guaranteed to mature later. Once a message matures, the worker will
attempt to deliver the `Notification` instance contained in that message. If that fails, the Notification will be
wrapped in another message and enqueued into the next queue, which is typically configured with a longer maturity.

As currently configured, the `dss-notify` lambda is invoked every three minutes. Like any other Lambda, its running
time is limited to 5min. This means that for a period of two minutes, two `dss-notify` lambda invocations will be
active concurrently. Each invocation will contribute one worker to each queue. The two workers assigned to each queue
will cooperate safely. The invocation rate of the `dss-notify` lambda is configurable, but should lie between 1min and
5min. Faster rates are disallowed by AWS, slower rates would lead to artificially long delays between notification
attempts, exceeding the configured maturity delays.

Workers periodically watch the remaining running time of the lambda invocation they were created in and avoid taking on
work that they know would outlive the lambda invocation.

Typically, every `dss-notify` invocation launches as many workers as there are queues. However, the number of workers
is configurable. If the configured number of workers is less than the number of queues, the notifier will assign the
workers to a weighted random choice among the queues, prioritizing longer queues. If the configured number of workers
is greater than the number of queues, the notifier will first ensure that every queue is assigned one worker and then
assign the remaining workers to a weighted random choice of queues, again prioritizing longer ones. In the former case,
it will take multiple Notifier instances to cover all queues.

The system currently uses SQS FIFO queues. SQS FIFO queues have strong ordering and uniqueness constraints that make
them desirable. But that's not the primary reason the system uses them. FIFO queues also make a best effort to maintain
fairness among message groups. The system assigns each DSS subscription to its own message group. This way a blanket
subscription that generates many notifications is less likely to drown out a more specific subscription for which fewer
notifications occur. However, the choice of FIFO queues is not baked into the design. It would be straight-forward to
change the system to use standard queues.

Full disclosure: the ordering constraints enforced by FIFO queues are obviously weakened in the likely case that more
than one worker consumes a queue. Furthermore, the unqiqueness constraint (aka "exactly-once delivery") of FIFO queues
is weakened by two rare, but unavoidable race conditions. 

1) It is possible for a failed notification to become visible on queue N while it is being enqueued in queue N+1.
Reversing the order of operations—deleting the message from queue N before enqueueing it in queue N+1—would risk the
loss of that notification if the worker thread is interrupted in between the two steps. In other words, the design
favors duplication over data loss.

2) When delivering a notification to a slow endpoint, it is possible that the endpoint considers the request a
success whereas the notifier sees it as a timeout. Once the notification attempt times out, the 200 HTTP status
response will be discarded, and the message will be enqueued in the next queue for another attempt.

Lastly, FIFO message groups also interfere with the invariant that messages mature in FIFO order. A call to SQS's
`ReceiveMessage` API on a FIFO may return a probabalistic sampling of messages from different message groups, therefore
violating the global FIFO ordering of the queue. This could cause a message with a later maturity being pulled ahead of
messages with earlier maturity. The worker would sleep longer and potentially miss the maturity of those other
messages. The intuition is that this is not going to be a significant efficieny problem.

There is a standalone test for `Notifier` and `Notification` that does not involve any lambda invocations but exercises
`Notifier.enqueue()` and `Notifier.run()` directly. It does use actual SQS queues which it sets up and tears down on
the fly run by invoking `Notifier.deploy()` and `Notifier.destroy()`. The latter method has a safe-guard against
deleting queues in deployments other than `test`. Sharing queues between tests like we do for buckets would be
prohibitively difficult. While uniquely named keys happily coexist in a bucket, messages in a queue obviously affect
each other.
