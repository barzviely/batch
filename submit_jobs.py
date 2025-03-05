import boto3
import argparse
import logging
import time
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def list_videos(bucket_name, prefix=''):
    """List all videos in an S3 bucket with an optional prefix"""
    try:
        s3 = boto3.client('s3')
        logger.info(f"Listing videos in s3://{bucket_name}/{prefix}")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        videos = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.lower().endswith(('.mp4', '.avi', '.mov', '.mkv')):
                    videos.append({
                        'path': f"s3://{bucket_name}/{key}",
                        'size': obj['Size']
                    })
            logger.info(f"Found {len(videos)} videos in bucket {bucket_name}")
        else:
            logger.warning(f"No objects found in bucket {bucket_name} with prefix {prefix}")
        
        return videos
    except Exception as e:
        logger.error(f"Error listing videos: {e}", exc_info=True)
        raise

def submit_batch_job(video_path, output_bucket, job_queue, job_definition, custom_folder=None):
    """Submit a batch job to process a video"""
    try:
        batch = boto3.client('batch', region_name='eu-north-1')
        
        # Extract video name for job naming
        video_name = video_path.split('/')[-1]
        
        # Sanitize job name to conform to AWS Batch naming requirements
        # Only allow letters, numbers, hyphens, and underscores
        import re
        sanitized_name = re.sub(r'[^a-zA-Z0-9\-_]', '-', video_name)
        # Ensure it's not too long (AWS Batch has a limit)
        if len(sanitized_name) > 100:
            sanitized_name = sanitized_name[:100]
        
        job_name = f"extract_frames-{sanitized_name}"
        
        # Create the command with arguments for the container - REMOVED FPS PARAMETER
        command = [
            '--input', video_path,
            '--output', f"s3://{output_bucket}"
        ]
        
        # Add custom folder name if specified
        if custom_folder:
            command.extend(['--folder-name', custom_folder])
        
        logger.info(f"Submitting job {job_name} with command: {command}")
        
        # Submit the job
        response = batch.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            containerOverrides={
                'command': command
            }
        )
        
        job_id = response['jobId']
        logger.info(f"Submitted job {job_id} for video {video_path}")
        return job_id
    except Exception as e:
        logger.error(f"Error submitting batch job for {video_path}: {e}", exc_info=True)
        raise

def check_job_status(job_ids):
    """Check the status of submitted batch jobs"""
    try:
        batch = boto3.client('batch', region_name='eu-north-1')
        
        # Get job descriptions for all job IDs
        response = batch.describe_jobs(jobs=job_ids)
        
        statuses = {}
        for job in response['jobs']:
            job_id = job['jobId']
            status = job['status']
            statuses[job_id] = status
            
            # If job failed, get the reason
            if status == 'FAILED':
                reason = job.get('statusReason', 'No reason provided')
                logger.error(f"Job {job_id} failed: {reason}")
                
                # Try to get log stream info
                if 'container' in job and 'logStreamName' in job['container']:
                    log_stream = job['container']['logStreamName']
                    logger.error(f"Check CloudWatch logs: {log_stream}")
        
        return statuses
    except Exception as e:
        logger.error(f"Error checking job status: {e}", exc_info=True)
        return {}

def main():
    parser = argparse.ArgumentParser(description='Submit video processing jobs to AWS Batch')
    parser.add_argument('--input-bucket', required=True, help='S3 bucket with input videos')
    parser.add_argument('--output-bucket', required=True, help='S3 bucket for output frames')
    parser.add_argument('--prefix', default='', help='Optional prefix filter for videos in bucket')
    parser.add_argument('--job-queue', default='VideoProcessingQueue', help='AWS Batch job queue name')
    parser.add_argument('--job-definition', default='VideoFrameExtraction', help='AWS Batch job definition name')
    parser.add_argument('--wait', action='store_true', help='Wait for jobs to complete')
    parser.add_argument('--custom-folder', help='Custom folder name for output files. If not specified, video name is used.')
    
    args = parser.parse_args()
    
    try:
        # List all videos in the bucket
        videos = list_videos(args.input_bucket, args.prefix)
        if not videos:
            logger.error(f"No videos found in bucket {args.input_bucket} with prefix {args.prefix}")
            sys.exit(1)
        
        logger.info(f"Found {len(videos)} videos to process")
        total_size_gb = sum(video['size'] for video in videos) / (1024 ** 3)
        logger.info(f"Total size of videos: {total_size_gb:.2f} GB")
        
        # Submit a job for each video
        job_ids = []
        for video in videos:
            job_id = submit_batch_job(
                video['path'], 
                args.output_bucket,
                args.job_queue,
                args.job_definition,
                args.custom_folder
            )
            job_ids.append(job_id)
        
        logger.info(f"Submitted {len(job_ids)} jobs to AWS Batch")
        
        # Wait for jobs to complete if requested
        if args.wait:
            logger.info("Waiting for jobs to complete...")
            all_complete = False
            
            while not all_complete:
                statuses = check_job_status(job_ids)
                logger.info(f"Job statuses: {statuses}")
                
                # Check if all jobs are in a terminal state
                terminal_states = ['SUCCEEDED', 'FAILED']
                all_complete = all(status in terminal_states for status in statuses.values())
                
                if not all_complete:
                    logger.info("Waiting 30 seconds before checking again...")
                    time.sleep(30)
            
            # Print final job statuses
            succeeded = sum(1 for status in statuses.values() if status == 'SUCCEEDED')
            failed = sum(1 for status in statuses.values() if status == 'FAILED')
            logger.info(f"All jobs completed: {succeeded} succeeded, {failed} failed")
        else:
            logger.info("Jobs submitted. Use AWS Management Console to monitor progress.")
            for job_id in job_ids:
                print(f"Job ID: {job_id}")
    
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()