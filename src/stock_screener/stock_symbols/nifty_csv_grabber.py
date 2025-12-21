import requests
import concurrent.futures

from typing import Set
from bs4 import BeautifulSoup


BASE_URL = "https://www.niftyindices.com"
ALL_INDEXES: Set = {
    "/indices/equity/broad-based-indices/nifty-100",
    "/indices/equity/broad-based-indices/nifty200",
    "/indices/equity/broad-based-indices/NIFTY--50",
    "/indices/equity/broad-based-indices/nifty500",
    "/indices/equity/broad-based-indices/nifty-india-fpi-150",
    "/indices/equity/broad-based-indices/nifty-largemidcap-250",
    "/indices/equity/broad-based-indices/nifty-microcap-250",
    "/indices/equity/broad-based-indices/NiftyMidcap100",
    "/indices/equity/broad-based-indices/niftymidcap150",
    "/indices/equity/broad-based-indices/niftymidcap50",
    "/indices/equity/broad-based-indices/nifty-midcap-select-index",
    "/indices/equity/broad-based-indices/niftymidsmallcap400",
    "/indices/equity/broad-based-indices/nifty-next-50",
    "/indices/equity/broad-based-indices/niftySmallcap100",
    "/indices/equity/broad-based-indices/niftysmallcap250",
    "/indices/equity/broad-based-indices/niftysmallcap50",
    "/indices/equity/broad-based-indices/nifty-total-market",
    "/indices/equity/broad-based-indices/nifty500-large-midsmall-equal-cap-weighted",
    "/indices/equity/broad-based-indices/nifty500-multicap-50-25-25-index",
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


class FileDownloader:
    def __init__(self, url: str, timeout: int=30, chunk_size: int=8192):
        self.headers = HEADERS
        self.url = url
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.filename = self._extract_file_name()

    def _extract_file_name(self) -> str:
        return self.url.split("/")[-1]

    def download_and_save_file(self) -> str:
        print(f"Downloading {self.url}...")
        response = requests.get(self.url, headers=self.headers, stream=True, timeout=self.timeout)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0

        with open("./csvs/"+self.filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=self.chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\rProgress: {progress:.1f}% ({downloaded}/{total_size})", end='')

        print(f"\nDownloaded to: {self.filename}")
        return self.filename


class LinkScraper:
    def __init__(self, url: str, timeout: int=15):
        self.url = url
        self.headers = HEADERS
        self.timeout = timeout

    @staticmethod
    def process_csv_link(link: str) -> str:
        return '/'.join([BASE_URL] + link.split('/')[3:])

    def download_file(self) -> None:
        try:
            resp = requests.get(self.url, headers=self.headers, timeout=self.timeout)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "lxml")
            anchor = soup.find("a", string="Index Constituent")
            link = ""
            if anchor and anchor.get("href"):
                link = anchor["href"]

            downloader = FileDownloader(LinkScraper.process_csv_link(link))
            filename = downloader.download_and_save_file()
            print(f"{filename} downloaded and saved.")

        except requests.RequestException as e:
            print(f"Error fetching {self.url}: {e}")


class NiftyIndexSaver:
    def __init__(self, max_workers: int = 20):
        self.base_url = BASE_URL
        self.all_index_urls = [self.base_url + index_url for index_url in ALL_INDEXES]
        self.max_workers = max_workers

    def scrape_and_download(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {
                executor.submit(LinkScraper(url=url).download_file)
                for url in self.all_index_urls
            }

            for future in concurrent.futures.as_completed(future_to_url):
                future.result()


if __name__ == "__main__":
    nifty_index_saver = NiftyIndexSaver()
    nifty_index_saver.scrape_and_download()
