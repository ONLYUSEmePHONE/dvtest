import datetime
import os

import xmltodict
import json
import paramiko
import boto3

class SSHCreds:
  def __init__(self, host, username, password, port=22, key_filename=None):
    self.host = host
    self.username = username
    self.password = password
    self.port = port
    self.key_filename = key_filename

def connect_ssh(ssh_client: paramiko.SSHClient, creds: SSHCreds):
  try:
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=creds.host, port=creds.port, username=creds.username, password=creds.password, key_filename=creds.key_filename)
  except Exception as e:
    print(e)
    return None
  #ftp = ssh_client.open_sftp()
  #files = ftp.listdir()

def disconnect_ssh(ssh_client):
  ssh_client.close()

def get_file(ftp, remote_path, local_path):
  try:
    ftp.get(remote_path, local_path)
    return True
  except Exception as e:
    print(e)
    return None

def read_xml(fpath):
  try:
    return xmltodict.parse(open(fpath, 'r').read())
  except Exception as e:
    print(e)
    return None

def get_modified_date(ftp, fpath):
  try:
    stat = ftp.stat(fpath)
    if stat is None:
      return None
    return str(datetime.datetime.fromtimestamp(stat.st_mtime).date())
  except Exception as e:
    print(e)
    return None

def process_sort(data, data_path):
  try:
    total_age = 0
    users = data["Users"]["User"]
    users_clean = []
    for user in users:
      age = user["UserAge"]
      total_age += int(age)
      users_clean.append({
        "UserID": user["UserID"],
        "UserName": user["UserName"],
        "UserAge": age,
        "EventTime": datetime.datetime.fromisoformat(user["EventTime"]).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"Z"
      })
    avg_age = total_age / len(users)
    above = "\n".join([json.dumps(user) for user in users_clean if int(user["UserAge"]) > avg_age])
    below = "\n".join([json.dumps(user) for user in users_clean if int(user["UserAge"]) <= avg_age])
    fabove = open(os.path.abspath(os.path.join(data_path, "above_average_output.json")), "w")
    fabove.write(above)
    fabove.close()
    fbelow = open(os.path.abspath(os.path.join(data_path, "below_average_output.json")), "w")
    fbelow.write(below)
    fbelow.close()
    return True
  except Exception as e:
    print(e)
    return None

def get_data_path(date):
  return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', date))

def make_data_dir(data_path):
  os.makedirs(data_path, exist_ok=True)

def upload_files(s3_client, data_path, bucketname, outputpath):
  try:
    s3_client.upload_file(os.path.join(data_path, "above_average_output.json"), bucketname, outputpath + 'above_average_output.json')
    s3_client.upload_file(os.path.join(data_path, "below_average_output.json"), bucketname, outputpath + 'below_average_output.json')
  except Exception as e:
    print(e)
    return None

def delete_file(ftp, fpath):
  try:
    ftp.remove(fpath)
  except Exception as e:
    print(e)
    return None

# main code

conn_data = {
  'SFTP Host': 'testFTP.dv.com',
  'SFTP User': 'testuser',
  'SFTP Password': '123456',
  'SFTP Path': '/data',
}

def main():
  # Open ssh and sftp connection
  ssh_client = paramiko.SSHClient()
  creds = SSHCreds(host=conn_data["SFTP Host"], username=conn_data["SFTP User"], password=conn_data["SFTP Password"])
  if connect_ssh(ssh_client, creds) is None:
    print("Error connecting via ssh")
    exit(1)
  try:
    ftp = ssh_client.open_sftp()
  except Exception as e:
    print(e)
    print("Error opening sftp")
    ssh_client.close()
    exit(1)

  # Utility for exiting providing a message and error code
  def uhoh(msg):
    print(msg)
    ftp.close()
    ssh_client.close()
    exit(1)

  # Check to see if /data file was created today
  mdate = get_modified_date(ftp, "/data")
  if mdate is None:
    uhoh("Error getting modification date for /data")
  if mdate != datetime.datetime.today():
    uhoh("/data was not created today")

  # Download the xml data from the sftp server
  local_path = os.path.abspath(os.path.join(get_data_path(mdate), "raw.xml"))
  make_data_dir(get_data_path(mdate))
  if get_file(ftp, "/data", local_path) is None:
    uhoh("Couldn't get file from remote server")

  # Read the contents of the downloaded xml file
  o = read_xml(local_path)
  if o is None:
    uhoh("Error reading xml file")

  # Process the xml to get json strings for records separated by user age
  if process_sort(o, get_data_path(mdate)) is None:
    uhoh("Error processing data")

  # Upload both json files to the S3 bucket
  s3_client = boto3.client(
    "s3",
    aws_access_key_id="testKeyString",
    aws_secret_access_key="testKeyString"
  )
  if upload_files(s3_client, get_data_path(mdate), "testbucket", "output/") is None:
    uhoh("Error uploading files to S3")

  # Delete the original data from the sftp server
  if delete_file(ftp, "/data") is None:
    uhoh("Error deleting original data from sftp server")

  # Gracefully exit the program
  print("Success 👐")
  ftp.close()
  ssh_client.close()
  exit(0)

if __name__ == "__main__":
  main()