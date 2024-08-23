import paramiko
import boto3
import os 
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
import dv.dv as dv
from dotenv import load_dotenv

def test_main():
  fileloc = "/home/ubuntu/data"
  load_dotenv()
  ssh_client = paramiko.SSHClient()
  creds = dv.SSHCreds(host=os.getenv("EC2_HOST"), username=os.getenv("EC2_USERNAME"), password=None, port=22, key_filename=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.test_keypair.pem')))
  dv.connect_ssh(ssh_client, creds)
  ftp = ssh_client.open_sftp()
  mdate = dv.get_modified_date(ftp, "/home/ubuntu/data")
  local_path = os.path.abspath(os.path.join(dv.get_data_path(mdate), "test_raw.xml"))
  dv.make_data_dir(dv.get_data_path(mdate))
  dv.get_file(ftp, fileloc, local_path)
  o = dv.read_xml(local_path)
  dv.process_sort(o, dv.get_data_path(mdate))
  s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET")
  )
  dv.upload_files(s3_client, dv.get_data_path(mdate), os.getenv("S3_BUCKET"), "output/")
  dv.delete_file(ftp, fileloc)
  ftp.close()
  ssh_client.close()
  print("Success 🤑")

if __name__ == "__main__":
  test_main()