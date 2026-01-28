import argparse
import os
from ftplib import FTP, error_perm


def main():
    parser = argparse.ArgumentParser(description="Upload files to FTP server.")
    parser.add_argument("--local", required=True, help="Local directory to upload")
    parser.add_argument("--remote", required=True, help="Remote directory on FTP server")
    parser.add_argument("--host", required=True, help="FTP server host")
    parser.add_argument("--user", required=True, help="FTP username")
    parser.add_argument("--passwd", required=True, help="FTP password")
    args = parser.parse_args()

    upload_all(args.local, args.remote, args.host, args.user, args.passwd)


def ftp_makedirs(ftp: FTP, remotedir):
    dirs = remotedir.strip('/').split('/')
    path = ''
    for d in dirs:
        if not d:
            continue
        path += '/' + d
        try:
            ftp.mkd(path)
        except error_perm as e:
            # Ignore error if directory already exists
            if not str(e).startswith("550"):
                raise


def ftp_clean_dir(ftp: FTP, remote_dir):
    try:
        ftp.cwd(remote_dir)
    except error_perm:
        return
    for item in ftp.nlst():
        try:
            ftp.delete(item)
            print(f"Deleted file: {item}")
        except error_perm:
            try:
                ftp_clean_dir(ftp, item)
                ftp.rmd(item)
                print(f"Deleted dir: {item}")
            except error_perm as e:
                print(f"Skip item: {item} -- {e}")


def ftp_upload_dir(ftp: FTP, local_dir, remote_dir):
    for root, dirs, files in os.walk(local_dir):
        rel_path = os.path.relpath(root, local_dir)
        rel_remote = remote_dir if rel_path == "." else remote_dir + "/" + rel_path.replace("\\", "/")
        ftp_makedirs(ftp, rel_remote)
        ftp.cwd(rel_remote)
        for f in files:
            full_path = os.path.join(root, f)
            with open(full_path, "rb") as fh:
                print(f"Uploading {full_path} -> {rel_remote}/{f}")
                ftp.storbinary(f"STOR {f}", fh)


def upload_all(local_dir, remote_dir, ftp_host, ftp_user, ftp_passwd):
    ftp = FTP(ftp_host)
    ftp.login(ftp_user, ftp_passwd)
    ftp_makedirs(ftp, remote_dir)
    ftp_clean_dir(ftp, remote_dir)
    ftp_upload_dir(ftp, local_dir, remote_dir)
    ftp.quit()


if __name__ == "__main__":
    main()
