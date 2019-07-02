import boto3
from botocore import UNSIGNED
from botocore.config import Config
from enum import Enum
import os


class OpenImageDataType(Enum):
    TRAIN = "train"
    TEST = "test"
    VALIDATION = "validation"


class OpenImageWrapper:
    def __init__(self, root_dst_folder="image"):
        self.__bucket_name = "open-images-dataset"
        self.__train_folder_name = "train"
        self.__val_folder_name = "validation"
        self.__test_folder_name = "test"
        self.__root_dst_folder = root_dst_folder

        # initialize
        os.makedirs(self.__root_dst_folder, exist_ok=True)
        self.__s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    def download(self, data_type):
        folder_name = data_type.value
        dst_folder = os.path.join(self.__root_dst_folder, folder_name)
        os.makedirs(dst_folder, exist_ok=True)

        # get image file list
        file_key_list = self.__get_file_list(folder_name)

        # download image files.
        file_count = len(file_key_list)
        for i, file_key in enumerate(file_key_list):
            print("{}/{} start to download file..".format(i+1, file_count))
            filename = os.path.basename(file_key)
            with open(os.path.join(dst_folder, filename), "wb") as file:
                self.__s3.download_fileobj(Bucket=self.__bucket_name, Key=file_key, Fileobj=file)

    def __get_file_list(self, prefix):
        file_key_list = []
        next_token = ""

        while True:
            if next_token == "":
                response = self.__s3.list_objects_v2(Bucket=self.__bucket_name, Prefix=prefix)
            else:
                response = self.__s3.list_objects_v2(Bucket=self.__bucket_name, Prefix=prefix, ContinuationToken=next_token)

            if "Contents" in response:
                contents = response["Contents"]

                for content in contents:
                    file_key_list.append(content["Key"])

            if "NextContinuationToken" in response:
                next_token = response['NextContinuationToken']
            else:
                break

        return file_key_list


if __name__ == "__main__":
    wrapper = OpenImageWrapper()
    wrapper.download(OpenImageDataType.VALIDATION)
