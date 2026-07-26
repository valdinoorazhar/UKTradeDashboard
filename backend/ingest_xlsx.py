import logging
import requests
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_xlsx(url, file_name, save_path):
    """
    Download a xlsx file from a URL and save it locally.

    Args:
        url: URL of the xlsx file to download
        file_name: Name of the file to save
        save_path: Folder where the file should be saved

    Returns:
        str: Full path to the saved xlsx file, or None if failed
    """
    try:
        logger.info(f"Downloading from: {url}")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        output_dir = Path(save_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        with open(f'{output_dir}/{file_name}', mode="wb") as file:
            file.write(response.content)  # Save the content of the response directly

        logger.info(f"xlsx saved to {output_dir}")

        return str(output_dir.resolve())

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading xlsx: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing xlsx: {e}")
        return None