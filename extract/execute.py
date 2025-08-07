import os, sys, requests, time
from zipfile import ZipFile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from utility.utility import setup_logging, format_time

def download_zip_file(logger,url,output_dir):
    response=requests.get(url, stream=True)
    os.makedirs(output_dir,exist_ok=True)
    if response.status_code==200:
        filename=os.path.join(output_dir,"downloaded.zip")
        with open(filename,"wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip: {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

def extract_zip_file(logger,zip_filename, output_dir):
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)
    
    logger.info(f"Extracted files written to :{output_dir}")
    logger.info("Removing the zip file")
    os.remove(zip_filename)


def fix_json_dict(logger,output_dir):
    import json
    file_path=os.path.join(output_dir,"dict_artists.json")
    with open(file_path,"r") as f:
        data=json.load(f)

    with open (os.path.join(output_dir,"fixed_da.json"), "w", encoding="utf-8") as f_out:
        for key, value in data.items():
            record={"id":key, "related_ids":value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
        logger.info(f"File {file_path} has been fixed and written to {output_dir} as fixed_da.json")
    logger.info("Removing the original file")
    os.remove(file_path)

if __name__=="__main__":
    logger=setup_logging("extract.log")

    if len(sys.argv)<2:
        logger.error("Extraction path is required")
        logger.error("Exame usage")
        logger.error("python3 execute.py /home/Kushal/Data/Extraction")
    else:
        try:
            logger.info("Starting extraction engine ...")
            start=time.time()

            EXTRACT_PATH=sys.argv[1]
            KAGGLE_URL="https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250710%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250710T022158Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=7a7823cff870cd143649195c882938a5e2057d1a6e8a6b6eb3a1a57a7309336407470ae07525ed6db51d27f90bdffa90eed294afa04ee4a4908fda3b5cd45d677df4fa93a7d83bb49f60795476e7603465dbe2e7889724e933787961e485ba76d4d1a49a42c7cfbb38a75c303f389b867067ca2b78468912e485f2ad4daedf062434df8ccba402a1a4f5009fa5374c8e4f176c859a418b628d903d11a639bb170d306901ffbac2e115609beddeb747386cab98a3900cf8149167f57ad20a733e8d29277a1a94f434bd7e5e34905f2dd4321b66c1ff54356254c05d072272e6d51dc9ed6d4352ea35afd7c53edf096598239d67b138ff5e28764bca644868886f"
            zip_filename=download_zip_file(logger,KAGGLE_URL,EXTRACT_PATH)
            extract_zip_file(logger,zip_filename, EXTRACT_PATH)
            fix_json_dict(logger,EXTRACT_PATH)

            end=time.time()
            logger.info("Extraction stage complete")
            logger.info(f"Total time taken: {format_time(end-start)}")
        except Exception as e:
            logger.error(f"Error: {e}")

