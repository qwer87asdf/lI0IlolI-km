#!/usr/bin/env python3
"""
R2 Upload Script for Cloudflare R2 Storage
Supports both direct S3-compatible uploads and Worker API uploads
Handles large files with multipart upload
"""

import os
import sys
import json
import hashlib
import mimetypes
import subprocess
from pathlib import Path
from typing import Optional, List, Dict
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm
from dotenv import load_dotenv
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    import pathspec
except ImportError:
    pathspec = None

# Load environment variables
load_dotenv()

class R2Uploader:
    def __init__(self):
        """Initialize R2 uploader with credentials from environment"""
        self.account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
        self.bucket_name = os.getenv('R2_BUCKET_NAME')
        self.access_key_id = os.getenv('R2_ACCESS_KEY_ID')
        self.secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
        self.worker_url = os.getenv('WORKER_API_BASE_URL')
        self.worker_auth = os.getenv('WORKER_AUTH_BEARER')
        self.s3_endpoint = os.getenv('R2_S3_ENDPOINT')
        
        # If endpoint is not provided, try to construct it from account ID
        if not self.s3_endpoint and self.account_id:
             self.s3_endpoint = f'https://{self.account_id}.r2.cloudflarestorage.com'

        # Validate configuration
        if not self.bucket_name:
            raise ValueError("Missing required R2 configuration: R2_BUCKET_NAME")
        
        if not self.s3_endpoint:
             raise ValueError("Missing required R2 configuration: R2_S3_ENDPOINT or CLOUDFLARE_ACCOUNT_ID")
        
        # Initialize S3 client for R2
        if self.access_key_id and self.secret_access_key:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                config=Config(
                    signature_version='s3v4',
                    retries={'max_attempts': 3}
                )
            )
        else:
            self.s3_client = None
            print("Warning: S3 credentials not configured. Only Worker API uploads available.")
    
    def _load_gitignore(self, base_dir: Path) -> Optional[object]:
        """
        Load .gitignore patterns from the base directory
        Returns a pathspec object that can match paths against gitignore patterns
        """
        gitignore_path = base_dir / '.gitignore'
        
        if not gitignore_path.exists():
            return None
        
        if pathspec is None:
            print("Warning: pathspec library not installed. Install with 'pip install pathspec'")
            print("Falling back to basic pattern matching.")
            return None
        
        try:
            with open(gitignore_path, 'r') as f:
                patterns = f.read().splitlines()
            
            # Filter out comments and empty lines
            patterns = [
                line.strip() for line in patterns 
                if line.strip() and not line.strip().startswith('#')
            ]
            
            return pathspec.PathSpec.from_lines('gitwildmatch', patterns)
        except Exception as e:
            print(f"Warning: Failed to load .gitignore: {e}")
            return None
    
    def _is_ignored(self, file_path: Path, base_dir: Path, gitignore_spec: Optional[object]) -> bool:
        """
        Check if a file should be ignored based on gitignore patterns
        """
        if gitignore_spec is None:
            return False
        
        try:
            # Get relative path from base directory
            relative_path = file_path.relative_to(base_dir)
            # Use forward slashes for consistency with gitignore patterns
            path_str = str(relative_path).replace(os.sep, '/')
            
            return gitignore_spec.match_file(path_str)
        except (ValueError, Exception):
            return False
    
    def upload_via_s3(self, file_path: str, key: Optional[str] = None, 
                      multipart_threshold: int = 100 * 1024 * 1024) -> bool:
        """
        Upload file using S3-compatible API
        Uses multipart upload for files > threshold (default 100MB)
        """
        if not self.s3_client:
            print("S3 client not configured. Use Worker API instead.")
            return False
        
        file_path = Path(file_path)
        if not file_path.exists():
            print(f"Error: File {file_path} not found")
            return False
        
        # Use provided key or generate from file path
        if not key:
            key = str(file_path).replace(os.sep, '/')
        
        file_size = file_path.stat().st_size
        
        try:
            # Use multipart upload for large files
            if file_size > multipart_threshold:
                print(f"Using multipart upload for {file_path.name} ({file_size / 1024 / 1024:.1f} MB)")
                return self._multipart_upload(file_path, key)
            else:
                # Regular upload for smaller files
                print(f"Uploading {file_path.name} ({file_size / 1024 / 1024:.1f} MB)")
                with open(file_path, 'rb') as f:
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=key,
                        Body=f,
                        ContentType=mimetypes.guess_type(str(file_path))[0] or 'application/octet-stream'
                    )
                print(f"✓ Uploaded {key}")
                return True
                
        except ClientError as e:
            print(f"Error uploading {file_path}: {e}")
            return False
    
    def _multipart_upload(self, file_path: Path, key: str, 
                         chunk_size: int = 10 * 1024 * 1024) -> bool:
        """
        Multipart upload for large files
        Default chunk size: 10MB
        """
        file_size = file_path.stat().st_size
        parts = []
        
        try:
            # Initiate multipart upload
            response = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                ContentType=mimetypes.guess_type(str(file_path))[0] or 'application/octet-stream'
            )
            upload_id = response['UploadId']
            
            # Upload parts
            with open(file_path, 'rb') as f:
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_path.name) as pbar:
                    part_number = 1
                    while True:
                        data = f.read(chunk_size)
                        if not data:
                            break
                        
                        response = self.s3_client.upload_part(
                            Bucket=self.bucket_name,
                            Key=key,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=data
                        )
                        
                        parts.append({
                            'PartNumber': part_number,
                            'ETag': response['ETag']
                        })
                        
                        pbar.update(len(data))
                        part_number += 1
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            print(f"✓ Multipart upload completed for {key}")
            return True
            
        except Exception as e:
            print(f"Error in multipart upload: {e}")
            # Abort multipart upload on error
            if 'upload_id' in locals():
                try:
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=key,
                        UploadId=upload_id
                    )
                except:
                    pass
            return False
    
    def sync_directory(self, local_dir: str, prefix: str = "", 
                      exclude_patterns: List[str] = None,
                      use_worker: bool = False,
                      max_workers: int = 5) -> Dict[str, int]:
        """
        Bidirectional sync between local directory and R2
        - Uploads files if local is newer or missing from remote
        - Downloads files if remote is newer or missing from local
        Returns dict with sync statistics
        """
        if not self.s3_client:
            print("S3 client not configured")
            return {}
        
        local_dir = Path(local_dir)
        if not local_dir.is_dir():
            print(f"Error: {local_dir} is not a directory")
            return {}
        
        # Load .gitignore patterns
        gitignore_spec = self._load_gitignore(local_dir)
        if gitignore_spec:
            print("✓ Loaded .gitignore patterns")
        
        # Default exclude patterns (in addition to gitignore)
        if exclude_patterns is None:
            exclude_patterns = ['.git', '__pycache__', '*.pyc', '.DS_Store']
        
        print("🔍 Analyzing local and remote files...")
        
        # Build local file map
        local_files = {}
        ignored_count = 0
        
        for file_path in local_dir.rglob('*'):
            if file_path.is_file():
                # Check if ignored by gitignore
                if self._is_ignored(file_path, local_dir, gitignore_spec):
                    ignored_count += 1
                    continue
                
                # Check additional exclude patterns
                if any(pattern in str(file_path) for pattern in exclude_patterns):
                    ignored_count += 1
                    continue
                
                relative_path = file_path.relative_to(local_dir)
                key = f"{prefix}/{relative_path}".replace(os.sep, '/').lstrip('/')
                local_files[key] = {
                    'path': file_path,
                    'mtime': file_path.stat().st_mtime,
                    'size': file_path.stat().st_size
                }
        
        if ignored_count > 0:
            print(f"⏭️  Ignored {ignored_count} files (gitignore + exclude patterns)")
        
        # Build remote file map
        print(f"📡 Fetching remote file list (prefix: '{prefix or '/'}')")
        remote_files = {}
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix or '')
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        remote_files[obj['Key']] = {
                            'mtime': obj['LastModified'].timestamp(),
                            'size': obj['Size']
                        }
        except ClientError as e:
            print(f"Error listing remote files: {e}")
            return {}
        
        print(f"📊 Local files: {len(local_files)}, Remote files: {len(remote_files)}")
        
        # Determine actions
        to_upload = []
        to_download = []
        
        # Check local files
        for key, local_info in local_files.items():
            if key not in remote_files:
                # File missing from remote - upload
                to_upload.append((local_info['path'], key, 'missing'))
            else:
                # Compare modification times
                remote_mtime = remote_files[key]['mtime']
                local_mtime = local_info['mtime']
                
                if local_mtime > remote_mtime:
                    # Local is newer - upload
                    to_upload.append((local_info['path'], key, 'newer'))
                elif remote_mtime > local_mtime:
                    # Remote is newer - download
                    to_download.append((key, local_info['path'], 'newer'))
        
        # Check for files only in remote (missing locally)
        for key in remote_files:
            if key not in local_files:
                # File missing from local - download
                local_path = local_dir / key.removeprefix(prefix).lstrip('/')
                to_download.append((key, local_path, 'missing'))
        
        print(f"\n📤 Files to upload: {len(to_upload)}")
        print(f"📥 Files to download: {len(to_download)}")
        
        if not to_upload and not to_download:
            print("\n✅ Everything is in sync!")
            return {'uploaded': 0, 'downloaded': 0, 'failed': 0}
        
        # Perform sync
        stats = {'uploaded': 0, 'downloaded': 0, 'failed': 0, 'upload_bytes': 0, 'download_bytes': 0}
        
        # Upload files
        if to_upload:
            print(f"\n{'='*50}")
            print("📤 Uploading files...")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                
                for file_path, key, reason in to_upload:
                    future = executor.submit(self._upload_file_quiet, file_path, key)
                    futures[future] = (file_path, key, reason)
                
                with tqdm(total=len(to_upload), desc="Uploading") as pbar:
                    for future in as_completed(futures):
                        file_path, key, reason = futures[future]
                        try:
                            success = future.result()
                            if success:
                                stats['uploaded'] += 1
                                stats['upload_bytes'] += file_path.stat().st_size
                                pbar.write(f"  ✓ {key} ({reason})")
                            else:
                                stats['failed'] += 1
                                pbar.write(f"  ✗ Failed: {key}")
                        except Exception as e:
                            stats['failed'] += 1
                            pbar.write(f"  ✗ Error uploading {key}: {e}")
                        pbar.update(1)
        
        # Download files
        if to_download:
            print(f"\n{'='*50}")
            print("📥 Downloading files...")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                
                for key, local_path, reason in to_download:
                    future = executor.submit(self.download_file, key, local_path)
                    futures[future] = (key, local_path, reason)
                
                with tqdm(total=len(to_download), desc="Downloading") as pbar:
                    for future in as_completed(futures):
                        key, local_path, reason = futures[future]
                        try:
                            success = future.result()
                            if success:
                                stats['downloaded'] += 1
                                stats['download_bytes'] += remote_files.get(key, {}).get('size', 0)
                                pbar.write(f"  ✓ {key} ({reason})")
                            else:
                                stats['failed'] += 1
                                pbar.write(f"  ✗ Failed: {key}")
                        except Exception as e:
                            stats['failed'] += 1
                            pbar.write(f"  ✗ Error downloading {key}: {e}")
                        pbar.update(1)
        
        # Print summary
        print(f"\n{'='*50}")
        print(f"Sync Summary:")
        print(f"  ⬆️  Uploaded: {stats['uploaded']} ({stats['upload_bytes'] / 1024 / 1024:.2f} MB)")
        print(f"  ⬇️  Downloaded: {stats['downloaded']} ({stats['download_bytes'] / 1024 / 1024:.2f} MB)")
        print(f"  ❌ Failed: {stats['failed']}")
        
        return stats
    
    def list_objects(self, prefix: str = "", max_keys: int = 1000) -> List[Dict]:
        """List objects in R2 bucket"""
        if not self.s3_client:
            print("S3 client not configured")
            return []
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            objects = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    objects.append({
                        'Key': obj['Key'],
                        'Size': obj['Size'],
                        'LastModified': obj['LastModified'].isoformat()
                    })
            
            return objects
            
        except ClientError as e:
            print(f"Error listing objects: {e}")
            return []
    
    def delete_object(self, key: str) -> bool:
        """Delete object from R2"""
        if not self.s3_client:
            print("S3 client not configured")
            return False

        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            print(f"✓ Deleted {key}")
            return True
        except ClientError as e:
            print(f"Error deleting {key}: {e}")
            return False

    def delete_prefix(self, prefix: str) -> Dict[str, int]:
        """Delete all objects under a prefix (folder)"""
        if not self.s3_client:
            print("S3 client not configured")
            return {}

        stats = {'deleted': 0, 'failed': 0}

        try:
            # Use paginator to handle large number of objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            objects_to_delete = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects_to_delete.append({'Key': obj['Key']})

            if not objects_to_delete:
                print(f"No objects found with prefix '{prefix}'")
                return stats

            print(f"Found {len(objects_to_delete)} objects to delete")

            # Delete in batches of 1000 (S3 API limit)
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i + 1000]
                try:
                    response = self.s3_client.delete_objects(
                        Bucket=self.bucket_name,
                        Delete={'Objects': batch, 'Quiet': False}
                    )

                    if 'Deleted' in response:
                        for deleted in response['Deleted']:
                            print(f"✓ Deleted {deleted['Key']}")
                            stats['deleted'] += 1

                    if 'Errors' in response:
                        for error in response['Errors']:
                            print(f"✗ Failed to delete {error['Key']}: {error['Message']}")
                            stats['failed'] += 1

                except ClientError as e:
                    print(f"Error deleting batch: {e}")
                    stats['failed'] += len(batch)

            print(f"\n{'='*50}")
            print(f"Delete Summary:")
            print(f"  Deleted: {stats['deleted']}")
            print(f"  Failed: {stats['failed']}")

            return stats

        except ClientError as e:
            print(f"Error listing objects: {e}")
            return stats

    def upload_all_tracked_files(self, prefix: str = "", max_workers: int = 5) -> Dict[str, int]:
        """
        Upload all git-tracked files (automatically respects .gitignore via git ls-files)
        Returns dict with upload statistics
        """
        if not self.s3_client:
            print("S3 client not configured")
            return {}

        # Get list of all tracked files using git ls-files
        # Note: git ls-files automatically respects .gitignore
        print("🔍 Getting git-tracked files (respects .gitignore)...")
        try:
            result = subprocess.run(
                ['git', 'ls-files'],
                capture_output=True,
                text=True,
                check=True
            )
            tracked_files = [f.strip() for f in result.stdout.strip().split('\n') if f.strip()]
        except subprocess.CalledProcessError as e:
            print(f"Error getting git tracked files: {e}")
            return {}
        except FileNotFoundError:
            print("Git is not installed or not in PATH")
            return {}

        # Filter out sensitive files that shouldn't be uploaded even if tracked
        exclude_patterns = ['.env']
        files_to_upload = []
        excluded_count = 0

        for file_path in tracked_files:
            # Skip excluded patterns (safety check for sensitive files)
            if any(pattern in file_path for pattern in exclude_patterns):
                excluded_count += 1
                continue

            path = Path(file_path)
            if path.exists() and path.is_file():
                # Calculate R2 key
                key = f"{prefix}/{file_path}".lstrip('/') if prefix else file_path
                files_to_upload.append((path, key))
        
        if excluded_count > 0:
            print(f"⏭️  Excluded {excluded_count} sensitive files (.env, etc.)")

        print(f"Found {len(files_to_upload)} files to upload")

        # Upload files in parallel
        stats = {'success': 0, 'failed': 0, 'total_size': 0}
        failed_files = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for file_path, key in files_to_upload:
                future = executor.submit(self._upload_file_quiet, file_path, key)
                futures[future] = (file_path, key)

            with tqdm(total=len(files_to_upload), desc="Uploading files") as pbar:
                for future in as_completed(futures):
                    file_path, key = futures[future]
                    try:
                        success = future.result()
                        if success:
                            stats['success'] += 1
                            stats['total_size'] += file_path.stat().st_size
                        else:
                            stats['failed'] += 1
                            failed_files.append(str(file_path))
                    except Exception as e:
                        print(f"\nError uploading {file_path}: {e}")
                        stats['failed'] += 1
                        failed_files.append(str(file_path))
                    pbar.update(1)

        # Print summary
        print(f"\n{'='*50}")
        print(f"Upload Summary:")
        print(f"  Successful: {stats['success']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Total Size: {stats['total_size'] / 1024 / 1024:.2f} MB")

        if failed_files:
            print(f"\nFailed files:")
            for f in failed_files:
                print(f"  - {f}")

        return stats

    def _upload_file_quiet(self, file_path: Path, key: str) -> bool:
        """Upload a single file without verbose output"""
        try:
            with open(file_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=f,
                    ContentType=mimetypes.guess_type(str(file_path))[0] or 'application/octet-stream'
                )
            return True
        except ClientError:
            return False
    
    def download_file(self, key: str, local_path: Path) -> bool:
        """Download a file from R2 to local filesystem"""
        if not self.s3_client:
            print("S3 client not configured")
            return False
        
        try:
            # Create parent directories if they don't exist
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Download the file
            self.s3_client.download_file(
                Bucket=self.bucket_name,
                Key=key,
                Filename=str(local_path)
            )
            return True
        except ClientError as e:
            print(f"Error downloading {key}: {e}")
            return False
    
    def get_object_metadata(self, key: str) -> Optional[Dict]:
        """Get metadata for an object in R2"""
        if not self.s3_client:
            return None
        
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return {
                'LastModified': response['LastModified'],
                'Size': response['ContentLength']
            }
        except ClientError:
            return None


def main():
    parser = argparse.ArgumentParser(description='Upload files to Cloudflare R2')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Upload file command
    upload_parser = subparsers.add_parser('upload', help='Upload a file')
    upload_parser.add_argument('file', help='File to upload')
    upload_parser.add_argument('--key', help='R2 object key (default: file path)')
    upload_parser.add_argument('--worker', action='store_true', help='Use Worker API instead of S3')
    
    # Sync directory command
    sync_parser = subparsers.add_parser('sync', help='Sync directory to R2')
    sync_parser.add_argument('directory', help='Directory to sync')
    sync_parser.add_argument('--prefix', default='', help='R2 prefix for uploaded files')
    sync_parser.add_argument('--exclude', nargs='+', help='Patterns to exclude')
    sync_parser.add_argument('--worker', action='store_true', help='Use Worker API instead of S3')
    sync_parser.add_argument('--workers', type=int, default=5, help='Number of parallel uploads')
    
    # List objects command
    list_parser = subparsers.add_parser('list', help='List objects in R2')
    list_parser.add_argument('--prefix', default='', help='Prefix to filter objects')
    list_parser.add_argument('--max', type=int, default=1000, help='Maximum objects to list')
    
    # Delete object command
    delete_parser = subparsers.add_parser('delete', help='Delete object from R2')
    delete_parser.add_argument('key', help='Object key to delete')

    # Delete prefix (folder) command
    delete_prefix_parser = subparsers.add_parser('delete-prefix', help='Delete all objects under a prefix (folder)')
    delete_prefix_parser.add_argument('prefix', help='Prefix (folder) to delete')

    # Upload all git-tracked files command
    upload_all_parser = subparsers.add_parser('upload-all', help='Upload all git-tracked files (respects .gitignore)')
    upload_all_parser.add_argument('--prefix', default='', help='R2 prefix for uploaded files')
    upload_all_parser.add_argument('--workers', type=int, default=5, help='Number of parallel uploads')

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize uploader
    try:
        uploader = R2Uploader()
    except ValueError as e:
        print(f"Configuration error: {e}")
        print("Please create a .env file with required configuration.")
        print("See .env.example for template.")
        return 1
    
    # Execute command
    if args.command == 'upload':
        if args.worker:
            success = uploader.upload_via_worker(args.file, args.key)
        else:
            success = uploader.upload_via_s3(args.file, args.key)
        return 0 if success else 1
    
    elif args.command == 'sync':
        stats = uploader.sync_directory(
            args.directory,
            prefix=args.prefix,
            exclude_patterns=args.exclude,
            use_worker=args.worker,
            max_workers=args.workers
        )
        return 0 if stats.get('failed', 0) == 0 else 1
    
    elif args.command == 'list':
        objects = uploader.list_objects(prefix=args.prefix, max_keys=args.max)
        if objects:
            print(f"Found {len(objects)} objects:")
            for obj in objects:
                size_mb = obj['Size'] / 1024 / 1024
                print(f"  {obj['Key']} ({size_mb:.1f} MB) - {obj['LastModified']}")
        else:
            print("No objects found")
        return 0
    
    elif args.command == 'delete':
        success = uploader.delete_object(args.key)
        return 0 if success else 1

    elif args.command == 'delete-prefix':
        stats = uploader.delete_prefix(args.prefix)
        return 0 if stats.get('failed', 0) == 0 else 1

    elif args.command == 'upload-all':
        stats = uploader.upload_all_tracked_files(
            prefix=args.prefix,
            max_workers=args.workers
        )
        return 0 if stats.get('failed', 0) == 0 else 1


if __name__ == '__main__':
    sys.exit(main() or 0)