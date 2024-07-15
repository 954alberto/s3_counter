use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client as S3Client, Error as S3Error};
use tokio::task;
use futures::future::join_all;
use tokio::sync::{Semaphore, Mutex};
use std::sync::Arc;
use std::fs::File;
use std::io::Write;
use std::env;

struct BucketResult {
    bucket_name: String,
    object_count: usize,
}

async fn count_objects(client: &S3Client, bucket_name: &str) -> Result<usize, S3Error> {
    let mut continuation_token = None;
    let mut object_count = 0;

    loop {
        let list_objects_resp = client
            .list_objects_v2()
            .bucket(bucket_name)
            .set_continuation_token(continuation_token.clone())
            .send()
            .await?;

        if let Some(contents) = list_objects_resp.contents {
            object_count += contents.len();
        }

        continuation_token = list_objects_resp.next_continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    Ok(object_count)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-west-2");
    let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region_provider)
        .load()
        .await;

    // Get AWS_PROFILE environment variable
    let aws_profile = env::var("AWS_PROFILE").unwrap_or_else(|_| "default".to_string());

    // Create output directory if it doesn't exist
    let output_dir = "reports";
    std::fs::create_dir_all(output_dir)?;

    // Create output file within the directory
    let file_name = format!("{}/{}_s3_report.txt", output_dir, aws_profile);
    let mut file = File::create(&file_name)?;

    // Initialize the S3 client
    let s3_client = S3Client::new(&config);

    let list_buckets_resp = s3_client.list_buckets().send().await?;

    if let Some(buckets) = list_buckets_resp.buckets {
        let semaphore = Arc::new(Semaphore::new(10)); // Limit to 10 concurrent tasks
        let results = Arc::new(Mutex::new(Vec::new())); // Shared results vector

        let mut tasks = Vec::new();

        for bucket in buckets {
            if let Some(bucket_name) = bucket.name.clone() {
                let client_clone = s3_client.clone();
                let bucket_name_clone = bucket_name.clone();
                let semaphore_clone = Arc::clone(&semaphore);
                let results_clone = Arc::clone(&results);
                let file_name_clone = file_name.clone();

                tasks.push(task::spawn(async move {
                    let _permit = semaphore_clone.acquire().await;
                    match count_objects(&client_clone, &bucket_name_clone).await {
                        Ok(object_count) => {
                            let mut results = results_clone.lock().await;
                            results.push(BucketResult {
                                bucket_name: bucket_name_clone,
                                object_count,
                            });
                        }
                        Err(err) => {
                            eprintln!(
                                "Failed to count objects in bucket {}: {}",
                                bucket_name_clone, err
                            );
                        }
                    }
                }));
            }
        }

        // Wait for all tasks to complete
        let _ = join_all(tasks).await;

        // Write results to file
        let results = results.lock().await;
        for result in results.iter() {
            writeln!(
                &mut file,
                "Bucket: {} - Object count: {}",
                result.bucket_name, result.object_count
            )?;
        }
    }

    writeln!(file)?;

    Ok(())
}
