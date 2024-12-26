import boto3

s3 = boto3.client('s3')

# Define your bucket and folder
bucket_name = "your-spark-bucket"
scripts_folder = "scripts"

# Define local scripts and their destination paths
local_scripts = {
    "audience_segmentation.py": "./audience_segmentation.py",
    "alternative_dates.py": "./alternative_dates.py",  
    "ad_recommendations.py": "./ad_recommendations.py" 
}

# Upload each script to the S3 bucket
for script_name, local_path in local_scripts.items():
    s3_path = f"{scripts_folder}/{script_name}"  # Destination path in S3
    try:
        s3.upload_file(local_path, bucket_name, s3_path)
        print(f"Uploaded {script_name} to s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"Failed to upload {script_name}: {e}")
