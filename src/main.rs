use std::collections::HashMap;

use bytes::Bytes;
use object_store::{memory::InMemory, path::Path as StorePath, GetOptions, ObjectStore};
use tokio::runtime;
use url::Url;

fn main() {
    let rt = runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Create an in-memory object store
        let object_store = InMemory::new();
        let store_path = StorePath::from("data/file1");
        let bytes = Bytes::from_static(b"hello");
        object_store.put(&store_path, bytes).await.unwrap();

        // Ask for the object's metadata
        let object_meta = object_store.head(&store_path).await.unwrap();
        assert_eq!(object_meta.size, 5);

        // We read a region and get the metadata in one go?
        let get_options = GetOptions {
            range: Some(0..2),
            ..Default::default()
        };
        let get_result = object_store
            .get_opts(&store_path, get_options)
            .await
            .unwrap();

        // ============================================================
        // NOTE: The metadata is the same as the one we got from `head`
        // ============================================================
        assert_eq!(get_result.meta.size, 5);
        assert_eq!(get_result.range.len(), 2);
        let bytes = get_result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"he"));

        // Next, we try to read from a public S3 bucket

        // arn:aws:s3:::ubc-sunflower-genome
        // AWS Region
        // us-west-2
        // AWS CLI Access (No AWS account required)
        // aws s3 ls --no-sign-request s3://ubc-sunflower-genome/
        // >aws s3 ls --no-sign-request s3://ubc-sunflower-genome/sequence/sra/PRJNA322345/SRR3648257.sra
        // 2020-04-05 16:22:18 7064209461 SRR3648257.sra

        // Any AWS credentials will do
        use rusoto_credential::{ProfileProvider, ProvideAwsCredentials};
        let credentials = ProfileProvider::new().unwrap().credentials().await.unwrap();
        let url = "s3://ubc-sunflower-genome/sequence/sra/PRJNA322345/SRR3648257.sra";
        let url = Url::parse(url).unwrap();
        let options: HashMap<&str, &str> = [
            ("aws_access_key_id", credentials.aws_access_key_id()),
            ("aws_secret_access_key", credentials.aws_secret_access_key()),
            ("aws_region", "us-west-2"),
        ]
        .iter()
        .cloned()
        .collect();
        let (object_store, store_path) = object_store::parse_url_opts(&url, options).unwrap();

        // Get the metadata
        let object_meta = object_store.head(&store_path).await.unwrap();
        assert_eq!(object_meta.size, 7064209461);

        // Can read a region and get the metadata in one go?
        let get_options = GetOptions {
            range: Some(0..2),
            ..Default::default()
        };
        let get_result = object_store
            .get_opts(&store_path, get_options)
            .await
            .unwrap();

        // ============================================================
        // NOTE: The metadata is NOT the same as the one we got from `head`.
        // `meta` is documented as "The [`ObjectMeta`] for this object".
        // However, it is not the metadata for the object, but the metadata
        // for the region we read.
        // ============================================================
        assert_eq!(get_result.meta.size, 2);
        let bytes = get_result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"NC"));
    })
}
