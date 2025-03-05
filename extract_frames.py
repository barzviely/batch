import os
import subprocess
import boto3
import argparse
import logging
from pathlib import Path
import watchtower
import shutil
import time
import concurrent.futures
from boto3.s3.transfer import TransferConfig

# Set up CloudWatch Logs handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Replace these with your CloudWatch Log Group and Stream names
log_group = 'video-processing-logs'  # Your CloudWatch Log Group name
log_stream = 'video-processing-stream'  # Your CloudWatch Log Stream name

# Create a logs client with region specified
logs_client = boto3.client('logs', region_name='eu-north-1')

# Create CloudWatch Logs handler using watchtower with the logs client
cloudwatch_handler = watchtower.CloudWatchLogHandler(
    log_group=log_group, 
    stream_name=log_stream,
    boto3_client=logs_client
)
logger.addHandler(cloudwatch_handler)

# Add a console handler for local debugging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# Configure S3 Transfer for multipart uploads
# Increase the multipart threshold and max concurrency for better performance
s3_config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,  # 8MB
    max_concurrency=20,  # Increased from 10 to 20 for better throughput
    use_threads=True,
    multipart_chunksize=16 * 1024 * 1024  # 16MB chunks for faster uploads
)

def check_disk_space(path, min_space_mb=1000):
    """Check if there's enough disk space available"""
    try:
        stat = os.statvfs(path)
        available_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
        logger.info(f"Available disk space at {path}: {available_mb:.2f} MB")
        return available_mb > min_space_mb
    except Exception as e:
        logger.error(f"Error checking disk space: {e}")
        return False

def download_video(s3_path, local_path):
    """Download a video from S3"""
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    s3 = boto3.client('s3', region_name='eu-north-1')
    logger.info(f"Downloading {s3_path} to {local_path}")
    
    # Check disk space before downloading
    if not check_disk_space('/tmp', 5000):  # At least 5GB free
        raise Exception("Not enough disk space to download video")
    
    try:
        s3.download_file(bucket, key, local_path)
        logger.info(f"Downloaded video to {local_path}")
        
        # Check file size
        file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
        logger.info(f"Video file size: {file_size_mb:.2f} MB")
    except Exception as e:
        logger.error(f"Error downloading video from S3: {e}")
        raise

def upload_frames_batch(frames, s3_bucket, s3_prefix, video_basename):
    """Upload a batch of frames to S3 in parallel"""
    s3 = boto3.client('s3', region_name='eu-north-1')
    uploaded = 0
    failed = 0
    
    # Create a thread pool for parallel uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # Create a dict of futures to frames for tracking
        future_to_frame = {}
        
        # Submit all upload tasks to the executor
        for frame in frames:
            frame_name = frame.name
            # Ensure prefix path is properly formed
            prefix_path = s3_prefix
            if s3_prefix and not s3_prefix.endswith('/'):
                prefix_path = f"{s3_prefix}/"
                
            s3_key = f"{prefix_path}{video_basename}/{frame_name}"
            
            future = executor.submit(
                s3.upload_file,
                str(frame),
                s3_bucket,
                s3_key,
                Config=s3_config
            )
            future_to_frame[future] = frame
        
        # Process the results as they complete
        for future in concurrent.futures.as_completed(future_to_frame):
            frame = future_to_frame[future]
            try:
                future.result()  # This will raise an exception if the upload failed
                uploaded += 1
                # Delete the frame after successful upload
                os.remove(frame)
            except Exception as e:
                logger.error(f"Failed to upload {frame.name}: {e}")
                failed += 1
    
    return uploaded, failed

def get_video_duration(video_path):
    """Get the duration of the video in seconds"""
    cmd = [
        'ffprobe', 
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1', 
        video_path
    ]
    
    try:
        duration = float(subprocess.run(
            cmd, 
            check=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        ).stdout.decode().strip())
        
        logger.info(f"Video duration: {duration:.2f} seconds")
        return duration
    except Exception as e:
        logger.warning(f"Could not determine video duration: {e}. Using default of 3600 seconds.")
        return 3600  # Default to 1 hour

def get_video_frame_rate(video_path):
    """Get the frame rate of the video"""
    probe_fps_cmd = [
        'ffprobe', 
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=r_frame_rate',
        '-of', 'default=noprint_wrappers=1:nokey=1', 
        video_path
    ]
    
    try:
        # This might return something like "30000/1001" for 29.97 fps
        fps_output = subprocess.run(
            probe_fps_cmd, 
            check=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        ).stdout.decode().strip()
        
        # Convert to float if needed
        if '/' in fps_output:
            num, den = map(int, fps_output.split('/'))
            frame_rate = num / den
        else:
            frame_rate = float(fps_output)
            
        logger.info(f"Video frame rate: {frame_rate} fps")
        return frame_rate
    except Exception as e:
        logger.warning(f"Could not determine frame rate: {e}. Using 30 fps as default.")
        return 30.0

def extract_frames_segment(video_path, output_dir, start_time, segment_duration, frame_interval=1):
    """
    Extract frames from a video segment with timestamps - optimized version
    
    Args:
        video_path: Path to the video file
        output_dir: Directory to save extracted frames
        start_time: Start time in seconds for this segment
        segment_duration: Duration in seconds for this segment
        frame_interval: Extract every Nth frame (1=all frames, 2=every other frame, etc.)
    
    Returns:
        List of paths to extracted frames
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract video filename from path
    video_filename = os.path.basename(video_path)
    video_name = os.path.splitext(video_filename)[0]
    
    # Create a segment-specific output directory to avoid filename conflicts
    segment_dir = f"{output_dir}/segment_{int(start_time)}"
    os.makedirs(segment_dir, exist_ok=True)
    
    # Optimized FFmpeg command:
    # 1. Place -ss before -i for faster seeking
    # 2. Use quality setting of 3 for good quality but faster processing
    # 3. Add -preset ultrafast for maximum speed
    cmd = [
        'ffmpeg',
        '-ss', str(start_time),  # Start time - placed BEFORE input for faster seeking
        '-i', video_path,
        '-t', str(segment_duration),  # Duration
        '-vsync', '0',
        '-q:v', '3',  # Quality setting (1=highest, 31=lowest) - using 3 for good quality
        '-preset', 'ultrafast',  # Use ultrafast preset for maximum speed
        '-vf', f'select=not(mod(n\,{frame_interval}))',  # Extract every Nth frame
        f'{segment_dir}/frame_%08d.jpg'
    ]
    
    logger.info(f"Extracting frames from segment starting at {start_time}s for {segment_duration}s")
    logger.info(f"Command: {' '.join(cmd)}")
    
    try:
        process = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Get frame rate to calculate timestamps
        frame_rate = get_video_frame_rate(video_path)
        
        # Rename files with timestamps
        frames = sorted(list(Path(segment_dir).glob('frame_*.jpg')))
        renamed_frames = []
        
        for i, frame in enumerate(frames):
            try:
                frame_num = int(frame.stem.split('_')[1])
                # Calculate time based on frame number, frame rate, and segment start time
                timestamp = start_time + (frame_num * frame_interval / frame_rate)
            except:
                # Fallback - estimate based on position in the list
                timestamp = start_time + (i * frame_interval / frame_rate)
            
            hours, remainder = divmod(timestamp, 3600)
            minutes, remainder = divmod(remainder, 60)
            seconds = int(remainder)
            milliseconds = int((remainder - seconds) * 1000)
            
            if hours > 0:
                new_name = f"{video_name}_{int(hours):02d}_{int(minutes):02d}_{seconds:02d}_{milliseconds:03d}.jpg"
            elif minutes > 0:
                new_name = f"{video_name}_{int(minutes):02d}_{seconds:02d}_{milliseconds:03d}.jpg"
            else:
                new_name = f"{video_name}_{seconds:02d}_{milliseconds:03d}.jpg"
                
            new_path = frame.parent / new_name
            frame.rename(new_path)
            renamed_frames.append(new_path)
        
        logger.info(f"Extracted and renamed {len(renamed_frames)} frames from segment")
        return renamed_frames
    
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg failed with error: {e}")
        logger.error(f"FFmpeg stderr: {e.stderr.decode()}")
        return []
    except Exception as e:
        logger.error(f"Error extracting frames from segment: {e}")
        return []

def process_segment(segment, num_segments, video_path, output_dir, s3_bucket, s3_prefix, frame_interval=1):
    """Process a single segment in the worker pool"""
    segment_duration = 120  # seconds
    start_time = segment * segment_duration
    
    video_name = os.path.basename(video_path)
    video_basename = os.path.splitext(video_name)[0]
    
    logger.info(f"Processing segment {segment+1}/{num_segments} (starting at {start_time}s)")
    
    # Clear output directory to free space
    temp_segment_dir = f"{output_dir}/segment_{int(start_time)}"
    if os.path.exists(temp_segment_dir):
        shutil.rmtree(temp_segment_dir)
    
    # Process this segment
    frames = extract_frames_segment(
        video_path, 
        output_dir, 
        start_time, 
        segment_duration,
        frame_interval
    )
    
    if not frames:
        logger.warning(f"No frames extracted from segment {segment+1}")
        return 0, 0
    
    # Upload frames from this segment in batches
    logger.info(f"Uploading {len(frames)} frames from segment {segment+1}")
    uploaded, failed = upload_frames_batch(frames, s3_bucket, s3_prefix, video_basename)
    
    # Clean up this segment's directory
    if os.path.exists(temp_segment_dir):
        shutil.rmtree(temp_segment_dir)
    
    logger.info(f"Completed segment {segment+1}/{num_segments}: Uploaded {uploaded} frames, Failed {failed} frames")
    return uploaded, failed

def process_video_in_parallel(video_path, output_dir, output_s3_path, frame_interval=1, max_workers=3):
    """
    Process a video in segments with parallel execution to improve performance
    
    Args:
        video_path: Path to the video file
        output_dir: Directory to save extracted frames temporarily
        output_s3_path: S3 path to upload frames to
        frame_interval: Extract every Nth frame (1=all frames, 2=every other frame, etc.)
        max_workers: Maximum number of parallel segment workers
    """
    # Parse the S3 bucket and prefix from the output path
    # FIX: Handle case when there's no prefix
    s3_parts = output_s3_path.replace("s3://", "").split("/", 1)
    s3_bucket = s3_parts[0]
    s3_prefix = s3_parts[1] if len(s3_parts) > 1 else ""
    
    logger.info(f"Using S3 bucket: {s3_bucket}, prefix: '{s3_prefix}'")
    
    # Get video duration
    duration = get_video_duration(video_path)
    
    # Process in 2-minute segments
    segment_duration = 120  # seconds
    
    # Calculate number of segments
    num_segments = int(duration / segment_duration) + 1
    logger.info(f"Processing video in {num_segments} segments with {max_workers} parallel workers")
    
    total_frames_processed = 0
    upload_failures = 0
    
    # Use ThreadPoolExecutor to process segments in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all segments for processing
        future_to_segment = {
            executor.submit(
                process_segment, 
                segment,
                num_segments,
                video_path, 
                output_dir, 
                s3_bucket, 
                s3_prefix, 
                frame_interval
            ): segment for segment in range(num_segments)
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_segment):
            segment = future_to_segment[future]
            try:
                uploaded, failed = future.result()
                total_frames_processed += uploaded
                upload_failures += failed
            except Exception as e:
                logger.error(f"Error processing segment {segment+1}: {e}")
    
    logger.info(f"Video processing complete. Total frames: {total_frames_processed}")
    if upload_failures > 0:
        logger.warning(f"Failed to upload {upload_failures} frames")
    
    return total_frames_processed

def main():
    parser = argparse.ArgumentParser(description='Extract frames from videos with improved reliability')
    parser.add_argument('--input', required=True, help='S3 path to input video')
    parser.add_argument('--output', required=True, help='S3 path to output directory')
    parser.add_argument('--frame-interval', type=int, default=1, 
                        help='Extract every Nth frame (1=all frames, 2=every other frame, etc.)')
    parser.add_argument('--region', default='eu-north-1', help='AWS region')
    parser.add_argument('--parallel-segments', type=int, default=5, 
                        help='Number of segments to process in parallel')
    
    args = parser.parse_args()
    
    # Set AWS region from argument
    os.environ['AWS_DEFAULT_REGION'] = args.region
    
    logger.info(f"Starting optimized frame extraction job for {args.input} to {args.output}")
    logger.info(f"Frame interval: {args.frame_interval}")
    logger.info(f"AWS Region: {args.region}")
    logger.info(f"Parallel segments: {args.parallel_segments}")
    
    # Create and check temp directories
    temp_video_dir = '/tmp/videos'
    temp_frames_dir = '/tmp/frames'
    os.makedirs(temp_video_dir, exist_ok=True)
    os.makedirs(temp_frames_dir, exist_ok=True)
    
    # Check available disk space
    if not check_disk_space('/tmp', 5000):  # At least 5GB free
        logger.error("Not enough disk space available to start processing")
        return 1
    
    video_name = args.input.split('/')[-1]
    local_video_path = f"{temp_video_dir}/{video_name}"
    
    try:
        # Download video from S3
        download_video(args.input, local_video_path)
        
        # Process video in segments and upload frames
        frames_processed = process_video_in_parallel(
            local_video_path, 
            temp_frames_dir, 
            args.output,
            args.frame_interval,
            args.parallel_segments
        )
        
        logger.info(f"Job complete. Processed {frames_processed} frames.")
        return 0
    
    except Exception as e:
        logger.error(f"Error processing video: {e}", exc_info=True)
        return 1
    
    finally:
        # Clean up temporary files
        logger.info("Cleaning up temporary files")
        
        if os.path.exists(local_video_path):
            os.remove(local_video_path)
            logger.info(f"Removed temporary video file {local_video_path}")
        
        # Clean up any remaining frame files
        frame_count = 0
        for frame in Path(temp_frames_dir).glob('**/*.jpg'):
            os.remove(frame)
            frame_count += 1
        
        if frame_count > 0:
            logger.info(f"Removed {frame_count} temporary frame files")
        
        # Remove temporary directories
        for temp_dir in Path(temp_frames_dir).glob('segment_*'):
            if temp_dir.is_dir():
                shutil.rmtree(temp_dir)

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)