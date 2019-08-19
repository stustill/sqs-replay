extern crate clap;
extern crate rusoto_core;
extern crate rusoto_sqs;

use std::thread::sleep;
use std::time::Duration;

use clap::{App, Arg};
use rusoto_core::credential::ProfileProvider;
use rusoto_core::{request::HttpClient, Region};
use rusoto_sqs::{
    DeleteMessageRequest, GetQueueUrlRequest, ListDeadLetterSourceQueuesRequest,
    ReceiveMessageRequest, SendMessageRequest, Sqs, SqsClient,
};

fn main() {
    let matches = App::new("sqs-replay")
        .version("1.0")
        .about("Replays DLQed messages")
        .author("Stuart Still")
        .arg(
            Arg::with_name("queue")
                .short("q")
                .long("queue")
                .value_name("QUEUE")
                .help("The dead letter queue to replay")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("delay")
                .short("d")
                .long("delay")
                .value_name("DELAY")
                .help("The time in milliseconds to wait between each message")
                .default_value("1000")
                .takes_value(true),
        )
        .get_matches();

    let dlq_name = String::from(
        matches
            .value_of("queue")
            .expect("Must specify a queue name"),
    );

    let credential_provider = create_credential_provider();
    let request_dispatcher = HttpClient::new().expect("failed to create request dispatcher");
    let sqs = SqsClient::new_with(request_dispatcher, credential_provider, Region::EuWest1);

    let dlq_url = lookup_url(&sqs, &dlq_name);
    let source_url = lookup_dlq_source_url(&sqs, &dlq_url);
    println!("Replaying \n  {} \n  => \n  {}", source_url, dlq_url);
    replay(
        &sqs,
        &dlq_url,
        &source_url,
        matches
            .value_of("delay")
            .expect("delay must be specified")
            .parse::<u64>()
            .expect("delay must be a number of milliseconds"),
    );
}

fn replay(sqs: &dyn Sqs, from_queue: &str, to_queue: &str, delay: u64) {
    loop {
        let messages_result = sqs
            .receive_message(ReceiveMessageRequest {
                attribute_names: None,
                max_number_of_messages: Some(1),
                message_attribute_names: None,
                queue_url: from_queue.to_string(),
                receive_request_attempt_id: None,
                visibility_timeout: None,
                wait_time_seconds: None,
            })
            .sync()
            .expect("could not read from queue");
        match messages_result.messages {
            Some(messages) => {
                for message in messages {
                    println!(
                        "Replaying message {}",
                        message.message_id.unwrap_or_else(|| String::from("no-id"))
                    );
                    let _send_result = sqs
                        .send_message(SendMessageRequest {
                            delay_seconds: None,
                            message_attributes: message.message_attributes,
                            message_body: message.body.expect("message received without body"),
                            message_deduplication_id: None,
                            message_group_id: None,
                            queue_url: to_queue.to_string(),
                        })
                        .sync()
                        .expect("failed to send");
                    let delete_result = sqs
                        .delete_message(DeleteMessageRequest {
                            queue_url: from_queue.to_string(),
                            receipt_handle: message.receipt_handle.expect("cannot delete message"),
                        })
                        .sync();
                    delete_result.expect("failed to delete")
                }
            }
            None => return,
        }
        sleep(Duration::from_millis(delay));
    }
}

fn lookup_url(sqs: &dyn Sqs, queue_name: &str) -> String {
    let get_queue_url_result = sqs
        .get_queue_url(GetQueueUrlRequest {
            queue_name: String::from(queue_name),
            queue_owner_aws_account_id: None,
        })
        .sync()
        .expect("failed to lookup queue");
    get_queue_url_result
        .queue_url
        .expect("queue does not exist")
}

fn lookup_dlq_source_url(sqs: &dyn Sqs, dlq_url: &str) -> String {
    let mut list_dead_letter_source_queues_result = sqs
        .list_dead_letter_source_queues(ListDeadLetterSourceQueuesRequest {
            queue_url: String::from(dlq_url),
        })
        .sync()
        .expect("error looking up DLQ source");
    if list_dead_letter_source_queues_result.queue_urls.len() > 1 {
        panic!("DLQs with multiple sources queues not supported");
    } else {
        list_dead_letter_source_queues_result.queue_urls.remove(0)
    }
}

fn create_credential_provider() -> ProfileProvider {
    let mut profile_provider = ProfileProvider::new().expect("failed to create profile provider");
    profile_provider.set_profile("mfa");
    profile_provider
}
